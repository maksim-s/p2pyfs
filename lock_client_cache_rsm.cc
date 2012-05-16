// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <time.h>
#include "rsm_client.h"
#include "handle.h"

cached_lock_rsm::cached_lock_rsm()
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&receive_cv, NULL);
  pthread_cond_init(&readfree_cv, NULL);
  pthread_cond_init(&free_cv, NULL);
  pthread_cond_init(&none_cv, NULL);
  pthread_cond_init(&cpempty_cv, NULL);

  _status = NONE;
  acquired = received = false;
  rif = ltype = access = lock_protocol::UNUSED;
  amiowner = false;
  partition = "";
}

cached_lock_rsm::~cached_lock_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&receive_cv);
  pthread_cond_destroy(&readfree_cv);
  pthread_cond_destroy(&free_cv);
  pthread_cond_destroy(&none_cv);
  pthread_cond_destroy(&cpempty_cv);
}

void cached_lock_rsm::set_status(lock_status s) 
{
  tprintf("====> state change %d => %d\n", _status, s);
  switch(s) {
  case NONE:
    ltype = access  = lock_protocol::UNUSED;
    pthread_cond_broadcast(&none_cv);
    break;
  case XFER:
    break;
  case UPGRADING:
    break;
  case ACQUIRING:
    break;
  case REVOKING:
    break;
  case LOCKED:
    if (ltype == lock_protocol::READ) {
      pthread_cond_broadcast(&readfree_cv);
    }
    break;
  case FREE:
    ltype = lock_protocol::UNUSED;
    pthread_cond_broadcast(&free_cv);
    pthread_cond_broadcast(&readfree_cv);
    break;
  }
  _status = s;
}

cached_lock_rsm::lock_status cached_lock_rsm::status()
{
  return _status;
}

static void *
revokethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->revoker();
  return 0;
}

static void *
transferthread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->transferer();
  return 0;
}

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::transfer, this, 
	      &lock_client_cache_rsm::transfer_handler);
  rlsrpc->reg(clock_protocol::receive, this, 
	      &lock_client_cache_rsm::receive_handler);
  rlsrpc->reg(clock_protocol::revoke, this, 
	      &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(clock_protocol::invalidate, this, 
	      &lock_client_cache_rsm::invalidate_handler);
  xid = 0;

  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoker_cv, NULL);
  pthread_cond_init(&transferer_cv, NULL);

  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &transferthread, (void *) this);
  VERIFY (r == 0);
}

lock_client_cache_rsm::~lock_client_cache_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoker_cv);
  pthread_cond_destroy(&transferer_cv);
}

// invalidates the lock + extent; must be reader
void
lock_client_cache_rsm::revoker()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (revokeset.empty()) {
      pthread_cond_wait(&revoker_cv, &m);
    }

    lock_protocol::lockid_t lid;
    std::string cid;

    std::map<lock_protocol::lock_protocol::lockid_t, std::string>::iterator it;
    it = revokeset.begin();
    assert(it->second.length() > 0); // There is a revoke to do
    lid = it->first;
    cid = it->second;
    revokeset.erase(it); // Remove from revokeset
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    tprintf("[%s] revoker -> starting %llu\n", id.c_str(), lid);

    while (true) {
      // If UPGRADING, go ahead with revoke!
      if (clck.status() == cached_lock_rsm::UPGRADING) {
	tprintf("[%s] revoker -> %llu UPGRADING\n", id.c_str(), lid);
	assert(clck.rif == lock_protocol::WRITE);
	break;
      }
      // If ACQUIRING, then must process after done ACQUIRING
      else if (clck.status() == cached_lock_rsm::ACQUIRING) {
	assert(clck.rif == lock_protocol::READ);
      }
      // Else, always process after FREE
      else {
	assert(clck.access == lock_protocol::READ);
	if (clck.status() == cached_lock_rsm::FREE) {
	  break;
	}
      }

      pthread_cond_wait(&clck.free_cv, &clck.m);
    }

    // FREE or UPGRADING
    assert(clck.status() == cached_lock_rsm::FREE ||
	   clck.status() == cached_lock_rsm::UPGRADING);

    // No local owners
    assert(clck.lowners.size() == 0 && 
	   clck.ltype == lock_protocol::UNUSED);

    // If FREE, set to REVOKING; must not be owner
    if (clck.status() == cached_lock_rsm::FREE) {
      assert(!clck.amiowner);
      clck.set_status(cached_lock_rsm::REVOKING);
    }

    if (lu) {
      lu->doevict(lid);
    }

    tprintf("[%s] revoker -> %llu sending revoke to %s\n", 
	    id.c_str(), lid, cid.c_str());

    handle h(cid);
    rpcc *cl = h.safebind();
    if (cl) {
      int ret;
      pthread_mutex_unlock(&clck.m);
      int r = cl->call(clock_protocol::revoke, lid, id, ret);
      pthread_mutex_lock(&clck.m);
      assert(r == clock_protocol::OK);
    }

    // If REVOKING, then set status to NONE, and no longer OWNER
    if (clck.status() == cached_lock_rsm::REVOKING) {
      assert(!clck.amiowner);
      clck.set_status(cached_lock_rsm::NONE);
    }

    pthread_mutex_unlock(&clck.m);
  }
}

void
lock_client_cache_rsm::transferer()
{
  while (true) {
    tprintf("[%s] transferer -> starting iteration...\n", id.c_str());
    pthread_mutex_lock(&m);
    while (transferset.empty()) {
      pthread_cond_wait(&transferer_cv, &m);
    }

    lock_protocol::lockid_t lid;
    int ret;

    unsigned int i, size;
    std::string contents;
    std::vector<request_t>::iterator rit;
    std::set<std::string> cp;
    std::set<std::string>::iterator wit;
    request_t t;

    std::string oldp;

    std::set<lock_protocol::lock_protocol::lockid_t>::iterator it;
    lid = *transferset.begin();
    transferset.erase(lid); // Remove from revokeset
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    tprintf("[%s] transferer -> got some %llu xfer request to process [%d, %d]\n", 
	    id.c_str(), lid, clck.rrequests.size(), clck.wrequests.size());

    while (true) {
      // If UPGRADING, go ahead with xfer iff it is to us!
      if (clck.status() == cached_lock_rsm::UPGRADING) {
	tprintf("[%s] transferer -> %llu UPGRADING\n", id.c_str(), lid);
	assert(clck.rif == lock_protocol::WRITE);

	if (clck.wrequests.front().id == id) {
	  break;
	}
	if (clck.amiowner && clck.rrequests.size() == 0) {
	  break;
	}
      }
      // If ACQUIRING, then must process after done ACQUIRING
      else if (clck.status() == cached_lock_rsm::ACQUIRING) {
	assert(clck.rif != lock_protocol::UNUSED);
      }
      // Else, always process after FREE
      else {
	assert(clck.amiowner);
	if (clck.status() == cached_lock_rsm::FREE) {
	  break;
	}
      }

      pthread_cond_wait(&clck.free_cv, &clck.m);
    }

    tprintf("[%s] transferer -> starting %llu\n", id.c_str(), lid);

    // Block any local threads
    if (clck.status() == cached_lock_rsm::FREE) {
      clck.set_status(cached_lock_rsm::XFER);
    }

    // Are there READs to process before WRITEs?
    if (clck.partition.length() == 0 && clck.wrequests.size() > 0) {
      goto writes;
    }

    // Process all the READs 
    if (clck.rrequests.size() == 0) {
      goto writes;
    }

    oldp = clck.partition;

    size = clck.rrequests.size();
    for (i = 0; i < size; i++) {
      t = *(clck.rrequests.begin()); // get first guy and delete
      clck.rrequests.erase(clck.rrequests.begin());

      // Change state to reflect READxfer
      clck.access = lock_protocol::READ;
      clck.copyset.insert(t.id);

      lu->dofetch(lid, contents);

      // Send READ xfer
      handle h(t.id);
      rpcc *cl = h.safebind();
      if (cl) {
	tprintf("[%s] transferer -> %llu send READ [%s, %d]\n", 
		id.c_str(), lid, t.id.c_str(), t.type);
	pthread_mutex_unlock(&clck.m);
	int r = cl->call(clock_protocol::receive, lid, contents, ret);
	pthread_mutex_lock(&clck.m);
	assert(r == clock_protocol::OK);
      }

      if (t.id == clck.partition) { // Processed partition?
	break;
      }
    }

  writes:
    if (clck.rrequests.size() > 0 &&
	oldp != clck.partition) {
      // Do nothing, skip WRITEs
    }
    if (clck.wrequests.size() == 0) {
      assert(clck.partition.length() == 0);
    }
    else {
      clck.partition = ""; // This is no longer our partition
      
      // Process one WRITE and then if still requests remain, reloop
      rit = clck.wrequests.begin();
      t = *rit;
      clck.wrequests.erase(rit++);
      
      // Invalidate first
      cp = clck.copyset;
      size = cp.size();
      
      for (wit = cp.begin(); wit != cp.end(); /*wit++*/) {
	std::string cid = *wit;;
	cp.erase(wit++);
      
	// Send invalidate
	handle h(cid);
	rpcc *cl = h.safebind();
	if (cl) {
	  tprintf("[%s] transferer -> %llu send invalidate [%s]\n", 
		  id.c_str(), lid, cid.c_str());
	  pthread_mutex_unlock(&clck.m);
	  int r = cl->call(clock_protocol::invalidate, lid, id, ret);
	  pthread_mutex_lock(&clck.m);
	  assert(r == clock_protocol::OK);
	}
      }
      
      // copyset shouldn't be larger than size!
      assert(size >= clck.copyset.size());
      
      // Wait for copyset to become empty
      while (clck.copyset.size() != 0) {
	pthread_cond_wait(&clck.cpempty_cv, &clck.m);
      }
      
      // Modify state to reflect WRITE xfer
      clck.access = lock_protocol::UNUSED;
      clck.amiowner = false;

      lu->dofetch(lid, contents);
      if (lu) {
	lu->doevict(lid);
      }
      
      if (clck.status() != cached_lock_rsm::UPGRADING) {
	clck.set_status(cached_lock_rsm::NONE);
	// People waiting on FREE for XFER
	pthread_cond_broadcast(&clck.readfree_cv);
	pthread_cond_broadcast(&clck.free_cv);
      }

      // Send write xfer now
      handle h(t.id);
      rpcc *cl = h.safebind();
      if (cl) {
	tprintf("[%s] transferer -> %llu send WRITE [%s, %d]\n", 
		id.c_str(), lid, t.id.c_str(), t.type);
	pthread_mutex_unlock(&clck.m);
	int r = cl->call(clock_protocol::receive, lid, contents, ret);
	pthread_mutex_lock(&clck.m);
	assert(r == clock_protocol::OK);
      }
    }

    if (clck.status() == cached_lock_rsm::XFER) {
      clck.set_status(cached_lock_rsm::FREE);
    }

    // Does the lock still have queued xfer requests?
    if (clck.wrequests.size() > 0 ||
	clck.rrequests.size() > 0) {
      tprintf("[%s] transferer -> %llu rescheduling [%d, %d]\n", 
	      id.c_str(), lid, clck.rrequests.size(), clck.wrequests.size());
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      transferset.insert(lid);
      pthread_mutex_unlock(&m);
    }
    else {
      tprintf("[%s] transferer -> %llu NOT rescheduling [%d, %d]\n", 
	      id.c_str(), lid, clck.rrequests.size(), clck.wrequests.size());
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      transferset.erase(lid);
      pthread_mutex_unlock(&m);
    }
  }
}

cached_lock_rsm&
lock_client_cache_rsm::get_lock(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&m);
  return lockset[lid];
}

lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid, 
			       lock_protocol::lock_type type)
{
  int r = lock_protocol::OK;
  cached_lock_rsm &clck = get_lock(lid);

  tprintf("[%s] acquire -> %llu, %d\n", id.c_str(), lid, type);

  pthread_mutex_lock(&clck.m);
 start:
  switch(clck.status()) {
  case cached_lock_rsm::XFER:
    tprintf("[%s] acquire -> %llu, %d [XFER]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::XFER) {
      if (type == lock_protocol::READ) {
	pthread_cond_wait(&clck.readfree_cv, &clck.m);
      }
      else {
	// NOTE: We could actually be acquiring a READ lock from server,
	// but doesn't matter, will trigger new acquire flow once done.
	pthread_cond_wait(&clck.free_cv, &clck.m);
      }
    }
    goto start;
  case cached_lock_rsm::ACQUIRING:
    // Some other thread has already sent out request to server
    tprintf("[%s] acquire -> %llu, %d [ACQUIRING]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::ACQUIRING) {
      if (type == lock_protocol::READ) {
	pthread_cond_wait(&clck.readfree_cv, &clck.m);
      }
      else {
	// NOTE: We could actually be acquiring a READ lock from server,
	// but doesn't matter, will trigger new acquire flow once done.
	pthread_cond_wait(&clck.free_cv, &clck.m);
      }
    }
    goto start;
  case cached_lock_rsm::UPGRADING:
    // Some other thread has already sent out request to server
    tprintf("[%s] acquire -> %llu, %d [UPGRADING]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::UPGRADING) {
      if (type == lock_protocol::READ) {
	pthread_cond_wait(&clck.readfree_cv, &clck.m);
      }
      else {
	pthread_cond_wait(&clck.free_cv, &clck.m);
      }
    }
    goto start;
  case cached_lock_rsm::LOCKED:
    // Own lock, but someother thread using it.
    tprintf("[%s] acquire -> %llu, %d [LOCKED]\n", id.c_str(), lid, type);
    assert(clck.ltype != lock_protocol::UNUSED);
    assert(clck.lowners.size() > 0);

    while (clck.status() == cached_lock_rsm::LOCKED) {
      if (clck.ltype == lock_protocol::READ && 
	  type == lock_protocol::READ) {
	clck.lowners.insert(pthread_self());
	// Other read waiters can also join in the action!
	pthread_cond_broadcast(&clck.readfree_cv);
	r = lock_protocol::OK;
	break;
      }
      // Doesn't matter what stype is. Even if it is READ and we want WRITE,
      // we will trigger a new acquire lock once we loop back.
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::FREE:
    // We own lock, and no one is using it.
    tprintf("[%s] acquire -> %llu, %d [FREE]\n", id.c_str(), lid, type);
    assert(clck.ltype == lock_protocol::UNUSED);
    if (clck.access == lock_protocol::WRITE || 
	(clck.access == lock_protocol::READ && type == lock_protocol::READ)) {
      clck.lowners.insert(pthread_self());
      clck.ltype = type;
      clck.set_status(cached_lock_rsm::LOCKED);
      r = lock_protocol::OK;
      break;
    }
    else {
      assert(type == lock_protocol::WRITE);
      assert(clck.access == lock_protocol::READ);
      clck.set_status(cached_lock_rsm::UPGRADING);
      goto sacquire; // Start acquire flow for WRITE
    }
  case cached_lock_rsm::REVOKING:
    // Release thread is releasing.
    tprintf("[%s] acquire -> %llu, %d [REVOKING]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::REVOKING) {
      pthread_cond_wait(&clck.none_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::NONE:
    // Don't own lock, ask server to grant ownership.
    tprintf("[%s] acquire -> %llu, %d [NONE]\n", id.c_str(), lid, type);
    clck.set_status(cached_lock_rsm::ACQUIRING);

  sacquire:
    tprintf("[%s] acquire -> %llu, %d [sacquire] %d\n", 
	    id.c_str(), lid, type, clck.rif);
    // Can't have two requests in flight
    assert(clck.rif == lock_protocol::UNUSED);

    int ret;
    clck.acquired = false;
    clck.received = false;
    clck.rif = type;

    tprintf("[%s] sacquire -> %llu, %d [%d]\n", id.c_str(), lid, 
	  type, clck.status());

    pthread_mutex_unlock(&clck.m);
    r = rsmc->call(lock_protocol::acquire, lid, id, 
		   (unsigned int) type, (lock_protocol::xid_t) 0x0, ret);
    pthread_mutex_lock(&clck.m);

    if (r == lock_protocol::XOK) {
      tprintf("[%s] acquire -> %llu, %d Got XOK from LS.\n", 
	      id.c_str(), lid, type);
      r = lock_protocol::OK; 
      clck.access = lock_protocol::WRITE; // have write lock from server
      assert(clck.lowners.size() == 0);
      clck.ltype = type;
      clck.rif = lock_protocol::UNUSED;
      clck.acquired = clck.received = clck.amiowner = true;

      // Initialize empty extent
      if (lu) {
        lu->doinit(lid);
      }

      clck.lowners.insert(pthread_self());
      clck.set_status(cached_lock_rsm::LOCKED);
      break;
    }

    assert(r == lock_protocol::OK);

    tprintf("[%s] acquire -> %llu, %d Got OK from LS {%d}\n", 
	    id.c_str(), lid, type, clck.received);

    while (!clck.received) {
      tprintf("[%s] acquire -> %llu, %d Waiting for receive\n", 
	      id.c_str(), lid, type);
      pthread_cond_wait(&clck.receive_cv, &clck.m);
    }

    tprintf("[%s] acquire -> %llu, %d Got lock + data! [%d]\n", 
	    id.c_str(), lid, type, clck.lowners.size());

    assert(clck.access == type);
    assert(clck.lowners.size() == 0);

    if (type == lock_protocol::WRITE) {
      clck.amiowner = true;
    }
    else {
      clck.amiowner = false;
    }

    clck.acquired = true;
    clck.copyset.clear();
    clck.rif = lock_protocol::UNUSED;
    clck.ltype = type;
    clck.lowners.insert(pthread_self());
    clck.set_status(cached_lock_rsm::LOCKED);
    break;
  default:
    break;
  }

  pthread_mutex_unlock(&clck.m);
  return r;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  int r;
  cached_lock_rsm &clck = get_lock(lid);

  tprintf("[%s] release -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

  if (!clck.lowners.count(pthread_self())) {    
    tprintf("[%s] release -> %llu Non-owner!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }
  
  if (clck.status() != cached_lock_rsm::LOCKED) {
    tprintf("[%s] release -> %llu Not locked!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  clck.lowners.erase(pthread_self());
  if (clck.lowners.size() == 0) {
    clck.ltype = lock_protocol::UNUSED;
    clck.set_status(cached_lock_rsm::FREE);
  }
  else {
    assert(clck.ltype == lock_protocol::READ);
    pthread_cond_broadcast(&clck.readfree_cv);
  }

  r = lock_protocol::OK;

 end:
  pthread_mutex_unlock(&clck.m);
  return r;
}

// (Ex)-Reader -> Owner
clock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid,
                                       std::string cid, int&)
{
  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  tprintf("[%s] revoke_handler -> %llu [access: %d, state: %d, cp: %d]\n", 
	  id.c_str(), lid, clck.access, clck.status(), clck.copyset.size());

  clck.copyset.erase(cid);

  if (clck.copyset.size() == 0) {
    pthread_cond_signal(&clck.cpempty_cv); // transferer() waiting
    pthread_mutex_unlock(&m);
  } 

  pthread_mutex_unlock(&clck.m);
  return clock_protocol::OK;
}

// Owner -> Reader
clock_protocol::status
lock_client_cache_rsm::invalidate_handler(lock_protocol::lockid_t lid, 
                                      std::string cid, int &)
{
  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);

  tprintf("[%s] invalidate_handler -> %llu [%d]\n", id.c_str(), lid,
	  clck.status());

  // Shouldn't receive invalidates > 1 times!
  assert(clck.status() != cached_lock_rsm::NONE);

  // access should be READ, else can't be in copyset
  assert(clck.access == lock_protocol::READ);

  if (clck.status() == cached_lock_rsm::ACQUIRING) {
    // READ must be in flight if ACQUIRING; WRITE won't be in copyset
    assert(clck.rif == lock_protocol::READ); 
  }
  else if (clck.status() == cached_lock_rsm::UPGRADING) {
    // WRITE must be in flight if UPGRADING (we were in clck.copyset)
    assert(clck.rif == lock_protocol::WRITE); 
  }

  pthread_mutex_unlock(&clck.m);

  tprintf("[%s] invalidate_hanlder -> adding %llu to revokeset\n", 
	  id.c_str(), lid);
  pthread_mutex_lock(&m);
  revokeset[lid] = cid;
  pthread_cond_signal(&revoker_cv); 
  pthread_mutex_unlock(&m);
  
  return clock_protocol::OK;
}

rlock_protocol::status
lock_client_cache_rsm::transfer_handler(lock_protocol::lockid_t lid, 
					unsigned int rtype,
					std::string rid, 
					int &)
{
  int r = rlock_protocol::OK;
  tprintf("[%s] transfer_handler -> %llu [rtype: %d, rid: %s]\n", 
	  id.c_str(), lid, 
	  rtype,  rid.c_str());

  assert(rtype != lock_protocol::UNUSED);

  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);

  assert(clck.status() != cached_lock_rsm::NONE);

  if (clck.status() == cached_lock_rsm::ACQUIRING) {
    // READ must be in flight if ACQUIRING; WRITE won't be in copyset
    assert(clck.rif == lock_protocol::WRITE); 
  }
  else if (clck.status() == cached_lock_rsm::UPGRADING) {
    // WRITE must be in flight if UPGRADING (we were in clck.copyset)
    assert(clck.rif == lock_protocol::WRITE); 
  }
  else {
    // Must be owner!
    assert(clck.amiowner);
  }

  tprintf("[%s] transfer_handler -> processing %llu [ %d, %s]\n", 
	  id.c_str(), lid, rtype, rid.c_str());

  request_t t;
  t.id = rid;
  t.type = (lock_protocol::lock_type) rtype;

  // At most two WRITE xfers
  assert(clck.wrequests.size() <= 2);

  if (rtype == lock_protocol::WRITE) {
    assert(clck.wrequests.size() <= 1);

    // Can only get > 1 WRITE if we were owner with READ access, tried
    // to UPGRADE and then someone else tried to ACQUIRE WRITE;
    if (clck.wrequests.size() != 0) {
      assert(clck.status() == cached_lock_rsm::UPGRADING);
      assert(clck.rif == lock_protocol::WRITE);
      assert(clck.access == lock_protocol::READ);
      assert(rid != id);
    }
    else {
      // Make partition
      if (clck.rrequests.size() > 0) {
	clck.partition = clck.rrequests.back().id;
      }
      else {
	clck.partition = "";
      }
    }
    clck.wrequests.push_back(t);
  }
  else {
    // There can be two batches <RRR><W(me)><RRR><W(other)>?
    if (clck.wrequests.size() > 0) {
      assert(clck.partition.length() != 0);
    }
    clck.rrequests.push_back(t);
  }

  pthread_mutex_unlock(&clck.m);
  pthread_mutex_lock(&m);
  tprintf("[%s] transfer_handler -> signalling after adding %llu [ %d, %s]\n", 
	  id.c_str(), lid, rtype, rid.c_str());
  transferset.insert(lid);
  pthread_cond_signal(&transferer_cv);
  pthread_mutex_unlock(&m);

  return r;
}

clock_protocol::status
lock_client_cache_rsm::receive_handler(lock_protocol::lockid_t lid, 
				       std::string data, int &)
{
  int r = rlock_protocol::OK;
  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);

  tprintf("[%s] receive_handler -> %llu Got [%d]\n", 
	  id.c_str(), lid, data.length());

  assert(clck.status() == cached_lock_rsm::ACQUIRING ||
	 clck.status() == cached_lock_rsm::UPGRADING); // Must be in acquiring

  assert(clck.rif != lock_protocol::UNUSED);

  if (lu) {
    lu->dopopulate(lid, data);
  }

  if (clck.rif == lock_protocol::READ) {
    assert(clck.rrequests.size() == 0); // No xfer reqs for READ rif
    assert(clck.wrequests.size() == 0);
    assert(!clck.amiowner);
  }

  clck.access = clck.rif;
  clck.copyset.clear(); // Optimistically clear?
  clck.received = true;
  pthread_cond_signal(&clck.receive_cv); // at most 1 thread waiting
  pthread_mutex_unlock(&clck.m);
  return r;
}
