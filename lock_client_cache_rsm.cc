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

  _status = NONE;
  acquired = received = false;
  myxid.cxid = myxid.sxid = 0;
  ltype = access = lock_protocol::UNUSED;
  amiowner = false;
  newowner = "";
}

cached_lock_rsm::~cached_lock_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&receive_cv);
  pthread_cond_destroy(&readfree_cv);
  pthread_cond_destroy(&free_cv);
  pthread_cond_destroy(&none_cv);
}

void cached_lock_rsm::set_status(lock_status s) 
{
  switch(s) {
  case NONE:
    ltype = access  = lock_protocol::UNUSED;
    pthread_cond_broadcast(&none_cv);
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

static void *
invalidatethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->invalidater();
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
  pthread_cond_init(&invalidater_cv, NULL);

  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &transferthread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &invalidatethread, (void *) this);
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
    std::map<lock_protocol::lock_protocol::lockid_t, std::string>::iterator it;
    it = revokeset.begin();
    assert(it->second.size() > 0); // There is a revoke to do
    lid = it->first;
    std::string cid = it->second;
    revokeset.erase(it); // Remove from revokeset
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    tprintf("[%s] revoker -> %llu\n", id.c_str(), lid);

    if (clck.status() == cached_lock_rsm::ACQUIRING) {
      assert(clck.access == lock_protocol::WRITE);
      tprintf("[%s] revoker -> %llu WRITE bypass\n", id.c_str(), lid);
      goto next;
    }

    // Must have completed acquire flow
    if (!clck.acquired) {
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      revokeset[lid] = cid;
      continue;
    }

    while(true) {
      if (clck.status() == cached_lock_rsm::ACQUIRING) {
        // The second acquire in flight MUST be WRITE
        assert(clck.access == lock_protocol::WRITE);
	tprintf("[%s] revoker -> %llu WRITE bypass\n", id.c_str(), lid);
        goto next;
      }

      if (clck.status() == cached_lock_rsm::FREE) {
        break;
      }

      // Wait till lock is FREE
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }

    clck.set_status(cached_lock_rsm::REVOKING);

  next:
    assert(clck.lowners.size() == 0 && clck.ltype == lock_protocol::UNUSED);

    if (lu) {
      lu->doevict(lid);
    }

    assert(!clck.amiowner);

    tprintf("[%s] revoker -> %llu sending revoke to %s\n", id.c_str(), lid,
	    cid.c_str());

    handle h(cid);
    rpcc *cl = h.safebind();
    if (cl) {
      int ret;
      pthread_mutex_unlock(&clck.m);
      int r = cl->call(clock_protocol::revoke, lid, id, ret);
      pthread_mutex_lock(&clck.m);
      assert(r == clock_protocol::OK);
    }

    tprintf("[%s] revoker -> %llu sent revoke to %s\n", id.c_str(), lid,
	    cid.c_str());

    // If not ACQUIRING, then set status to NONE
    if (clck.status() == cached_lock_rsm::REVOKING) {
      clck.set_status(cached_lock_rsm::NONE);
    }
    pthread_mutex_unlock(&clck.m);
  }
}

// must process READ clck.requests before WRITEs
void
lock_client_cache_rsm::transferer()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (transferset.size() == 0) {
      pthread_cond_wait(&transferer_cv, &m);
    } 
    tprintf("[%s] transferer waking up...\n", id.c_str());
    std::map<lock_protocol::lockid_t, int>::iterator it = transferset.begin();
    lock_protocol::lockid_t lid = it->first;
    transferset.erase(it);
    pthread_mutex_unlock(&m);

    cached_lock_rsm& clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    // Haven't completed write flow? Process later...
    if (clck.status() == cached_lock_rsm::ACQUIRING) {
      tprintf("[%s] transferer -> %llu WRITE bypass\n", id.c_str(), lid);
      assert(clck.access == lock_protocol::WRITE);
      assert(!clck.acquired);
      goto redo;
    }

    if (!clck.acquired) { // Must finish acquire
      assert(clck.status() == cached_lock_rsm::ACQUIRING);
      assert(clck.access == lock_protocol::WRITE);
    redo:
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      transferset[lid]++;
      pthread_mutex_unlock(&m);
      continue;
    }

    tprintf("[%s] transferer -> processing %llu [%d]\n", id.c_str(), lid,
	   clck.requests.size());

    std::map<std::string, request_t>::iterator it2;

    // Service all READ requests
    for (it2 = clck.requests.begin(); 
	 it2 != clck.requests.end(); 
	 /*it2++*/) {
      if ((it2->second).type == lock_protocol::WRITE) {
	it2++;
	continue;
      }
      std::string rid = it2->first;
      request_t req = it2->second;
      std::string contents;
      lu->dofetch(lid, contents);

      tprintf("[%s] transferer -> %llu servicing READ to %s\n", id.c_str(), lid, 
	     rid.c_str());

      while(true) {
	if (clck.status() == cached_lock_rsm::ACQUIRING) {
	  // The second acquire in flight MUST be WRITE
	  assert(clck.access == lock_protocol::WRITE);
	  tprintf("[%s] transferer -> %llu WRITE bypass\n", id.c_str(), lid);
	  break;
	}

	if (clck.status() == cached_lock_rsm::FREE) {
	  break;
	}
	
	// Wait till lock is FREE
	pthread_cond_wait(&clck.free_cv, &clck.m);
      }
 
      clck.copyset.insert(rid);
      clck.access = lock_protocol::READ;

      assert(req.xid == clck.xids[rid].cxid);
      clck.xids[rid].sxid = req.xid; // change successful xid
      clck.requests.erase(it2++); // remove before call
	
      handle h(rid);
      rpcc *cl = h.safebind();
      if (cl) {
	int ret;
	tprintf("[%s] transferer -> %llu send READ [%llu, %llu]\n", 
	       id.c_str(), lid, req.xid,
	       clck.xids[rid].cxid);
	pthread_mutex_unlock(&clck.m);
	int r = cl->call(clock_protocol::receive, lid, clck.xids[rid].sxid, 
			 contents, ret);
	pthread_mutex_lock(&clck.m);
	assert(r == clock_protocol::OK);
      }
    }

    tprintf("[%s] transferer -> processing %llu checking WRITE [%d]\n", 
	   id.c_str(), lid,
	   clck.requests.size());

    // Service WRITE, if left
    it2 = clck.requests.begin();
    if (clck.requests.size() == 1 && 
	(it2->second).type == lock_protocol::WRITE) {
          
      assert(clck.copyset.size() == 0);
      assert(clck.amiowner == true);
      assert(clck.newowner.length() == 0);

      while(true) {
	if (clck.status() == cached_lock_rsm::FREE) {
	  break;
	}
	
	// Wait till lock is FREE
	pthread_cond_wait(&clck.free_cv, &clck.m);
      }

      clck.set_status(cached_lock_rsm::NONE);

      std::string contents;
      lu->dofetch(lid, contents);
      lu->doevict(lid);

      std::string rid = it2->first;
      request_t req = it2->second;

      printf("[%s] transferer -> %llu servicing WRITE to %s\n", id.c_str(), 
	     lid, rid.c_str());

      clck.amiowner = false;
      clck.newowner = rid;
      
      clck.requests.erase(it2++); // remove before call

      assert(clck.requests.size() == 0);

      handle h(rid);
      rpcc *cl = h.safebind();
      if (cl) {
	int ret;
	assert(req.xid == clck.xids[rid].cxid);
	clck.xids[rid].sxid = req.xid; // change successful xid
	pthread_mutex_unlock(&clck.m);
	int r = cl->call(clock_protocol::receive, lid, req.xid, contents, ret);
	pthread_mutex_lock(&clck.m);
	assert(r == clock_protocol::OK);
      }
    } 
    pthread_mutex_unlock(&clck.m);
  }
}

// must wait for clck.requests.size() == 1 and that request be WRITE
void
lock_client_cache_rsm::invalidater()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (invalidateset.size() == 0) {
      pthread_cond_wait(&invalidater_cv, &m); 
    }

    std::map<lock_protocol::lockid_t, int>::iterator it = invalidateset.begin();
    lock_protocol::lockid_t lid = it->first;
    assert(it->second > 0);
    invalidateset.erase(it);
    pthread_mutex_unlock(&m);

    cached_lock_rsm& clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    tprintf("[%s] invalidater -> processing %llu [rq: %d, cs: %d]\n", 
	   id.c_str(), lid,
	   clck.requests.size(), clck.copyset.size());

    // Have I finished WRITE flow?
    // Haven't completed write flow? Process later...
    if (clck.status() == cached_lock_rsm::ACQUIRING) {
      tprintf("[%s] transferer -> %llu WRITE bypass\n", id.c_str(), lid);
      assert(clck.access == lock_protocol::WRITE);
      assert(!clck.acquired);
      goto redo;
    }

    if (!clck.acquired) { // Must finish acquire
      assert(clck.status() == cached_lock_rsm::ACQUIRING);
      assert(clck.access == lock_protocol::WRITE);
    redo:
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      invalidateset[lid]++;
      pthread_mutex_unlock(&m);
      continue;
    }

    if (clck.requests.size() == 1 && clck.requests.begin()->second.type == 
        lock_protocol::WRITE) {
      std::set<std::string> cp = clck.copyset;      
      std::set<std::string>::iterator it;
      for (it = cp.begin(); it != cp.end(); it++) {
	tprintf("[%s] invalidater -> %llu sending invalidate %s\n", 
	       id.c_str(), lid, (*it).c_str());
        handle h(*it);
        rpcc *cl = h.safebind();
        if (cl) {
          int ret;
          pthread_mutex_unlock(&clck.m);
          int r = cl->call(clock_protocol::invalidate, lid, 
			   clck.xids[*it].sxid, 
			   id, ret);
          pthread_mutex_lock(&clck.m);
          assert(r == clock_protocol::OK);
        }
      }

      pthread_mutex_unlock(&clck.m);
    }
    else {
      pthread_mutex_unlock(&clck.m);
      pthread_mutex_lock(&m);
      invalidateset[lid]++;
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

// Assumes mutex held for clck
// Can be called by revoker only!
lock_protocol::status
lock_client_cache_rsm::srelease(lock_protocol::lockid_t lid, 
				cached_lock_rsm& clck)
{
  return 0;
}


lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid, 
			       lock_protocol::lock_type type)
{
  int r = lock_protocol::OK;
  lock_protocol::xid_t myxid;
  pthread_mutex_lock(&m);
  xid++; // xid must be globally protected
  myxid = xid;
  pthread_mutex_unlock(&m);

  cached_lock_rsm &clck = get_lock(lid);

  tprintf("[%s] acquire -> %llu, %d\n", id.c_str(), lid, type);

  pthread_mutex_lock(&clck.m);
 start:
  switch(clck.status()) {
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
      // Doesn't matter what access we have. 
      // Even if it is READ and we want WRITE,
      // we will trigger a new acquire flow once we loop back.
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::FREE:
    // We own lock, and no one is using it.
    tprintf("[%s] acquire -> %llu, %d [FREE]\n", id.c_str(), lid, type);
    assert(clck.ltype == lock_protocol::UNUSED);
    assert(clck.lowners.size() == 0);

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
  sacquire:
    clck.set_status(cached_lock_rsm::ACQUIRING);
    clck.access = type;
    
    int ret;
    clck.acquired = false;
    clck.received = false;
    clck.myxid.cxid = myxid;
    pthread_mutex_unlock(&clck.m);

    // Update xid, incase have to retry
    pthread_mutex_lock(&m);
    xid++; // xid must be globally protected
    myxid = xid;
    pthread_mutex_unlock(&m);
    tprintf("[%s] acquire -> sacquire %llu, %d\n", id.c_str(), lid, type);
    r = rsmc->call(lock_protocol::acquire, lid, id, 
		 (unsigned int) type, clck.myxid.cxid, ret);
    pthread_mutex_lock(&clck.m);

    if (r == lock_protocol::RETRY) goto sacquire;

    // HACK: No owner at server, so we are the owners!
    if (r == lock_protocol::XOK) {
      tprintf("[%s] acquire -> %llu, %d Got XOK from LS.\n", 
	      id.c_str(), lid, type);
      r = lock_protocol::OK; 
      clck.myxid.sxid = clck.myxid.cxid;
      clck.amiowner = true;
      clck.access = lock_protocol::WRITE; // have write lock from server
      assert(clck.lowners.size() == 0);
      clck.ltype = type;
      clck.lowners.insert(pthread_self());
      clck.set_status(cached_lock_rsm::LOCKED);

      // Initialize empty extent
      if (lu) {
        lu->doinit(lid);
      }
      break;
    }

    assert(r == lock_protocol::OK);

    tprintf("[%s] acquire -> %llu, %d Got OK from ls {%d}\n", 
	    id.c_str(), lid, type, clck.received);

    while (!clck.received) {
      tprintf("[%s] acquire -> %llu, %d Waiting for receive {%d}\n", 
	      id.c_str(), lid, type, clck.received);
      pthread_cond_wait(&clck.receive_cv, &clck.m); //timewait?
    }

    tprintf("[%s] acquire -> %llu, %d Got lock!\n", id.c_str(), lid, type);

    // Now we have lock + data!
    assert(clck.access == type);
    assert(clck.lowners.size() == 0);
    clck.ltype = type;
    clck.lowners.insert(pthread_self());
    clck.set_status(cached_lock_rsm::LOCKED);
    break;
  default:
    break;
  }

  clck.acquired = true;

  if (type == lock_protocol::WRITE) {
    assert(clck.amiowner); // receive_handler must set this!
    assert(clck.copyset.size() == 0);
  }

  // If any pending requests, process them! TODO: FIX THIS!
  if (clck.requests.size() > 0) {
    pthread_mutex_unlock(&clck.m);
    pthread_mutex_lock(&m);
    transferset[lid]++;
    pthread_cond_signal(&transferer_cv);
    pthread_mutex_unlock(&m);
  }
  else {
    pthread_mutex_unlock(&clck.m);
  }

  return r;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  int r;
  cached_lock_rsm &clck = get_lock(lid);

  tprintf("[%s] release -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

  if (clck.lowners.count(pthread_self()) == 0) {    
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
  tprintf("[%s] revoke_handler -> %llu [%d]\n", id.c_str(), lid,
	  clck.copyset.size());
  clck.copyset.erase(cid);
  if (clck.copyset.size() == 0) {
    assert(clck.requests.size() == 1);
    std::map<std::string, request_t>::iterator it = clck.requests.begin();
    assert((it->second).type == lock_protocol::WRITE);

    pthread_mutex_unlock(&clck.m);
    pthread_mutex_lock(&m);
    transferset[lid]++;
    pthread_cond_signal(&transferer_cv); // At most one thread waiting
    pthread_mutex_unlock(&m);
  } 
  else {
    pthread_mutex_unlock(&clck.m);
  }

  return clock_protocol::OK;
}

// Owner -> Reader
clock_protocol::status
lock_client_cache_rsm::invalidate_handler(lock_protocol::lockid_t lid, 
				      lock_protocol::xid_t xid,
                                      std::string cid, int &)
{
  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  tprintf("[%s] invalidate_handler -> %llu [%llu, %llu]\n", id.c_str(), lid,
	  xid, clck.myxid.sxid);

  lock_protocol::xid_t xidtocheck = clck.myxid.sxid;

  // What if READ acquire flow still unfinished?
  if (clck.status() == cached_lock_rsm::ACQUIRING && 
      clck.access == lock_protocol::READ) {
    xidtocheck = clck.myxid.cxid;
  }

  tprintf("[%s] invalidate_handler -> %llu [xtc: %llu]\n", id.c_str(), lid,
	  xidtocheck);

  bool invalidate = (xidtocheck == xid && 
		     clck.status() != cached_lock_rsm::NONE);

  if (invalidate && clck.status() != cached_lock_rsm::ACQUIRING) {
    assert(clck.access == lock_protocol::READ);
  }

  pthread_mutex_unlock(&clck.m);

  if (invalidate) {
    tprintf("[%s] invalidate_hanlder -> processing %llu\n", id.c_str(), lid);
    pthread_mutex_lock(&m);
    revokeset[lid] = cid;
    pthread_cond_signal(&revoker_cv); 
    pthread_mutex_unlock(&m);
  }

  return clock_protocol::OK;
}

rlock_protocol::status
lock_client_cache_rsm::transfer_handler(lock_protocol::lockid_t lid, 
					lock_protocol::xid_t xid, 
					unsigned int rtype,
					std::string rid, 
					lock_protocol::xid_t rxid,
					std::string &ret)
{
  int r = rlock_protocol::OK;
  tprintf("[%s] transfer_handler -> %llu, %d, %s\n", id.c_str(), lid, rtype, 
	   rid.c_str());

  assert(rtype != lock_protocol::UNUSED);

  cached_lock_rsm &clck = get_lock(lid);

  pthread_mutex_lock(&clck.m);

  // Haven't completed acquire yet?
  if (clck.myxid.sxid != xid) {
    assert(clck.myxid.sxid < xid);
    goto process;
  }

  if (clck.amiowner) {
    goto process;
  }

  // Already given up ownership?
  if (clck.status() == cached_lock_rsm::NONE) {
     assert(!clck.amiowner);
     tprintf("[%s] transfer_handler -> %llu, %d, %s [NONE CASE]\n", 
	     id.c_str(), 
	     lid, rtype, 
	     rid.c_str());
     ret = clck.newowner;
     pthread_mutex_unlock(&clck.m);
     return rlock_protocol::RETRY;
  }

  // Already given up ownership and made new request?
  if (clck.myxid.cxid > xid) {
    assert(clck.status() == cached_lock_rsm::ACQUIRING);
    assert(!clck.amiowner);
     tprintf("[%s] transfer_handler -> %llu, %d, %s [xid<cxid CASE]\n", 
	     id.c_str(), 
	     lid, rtype, 
	     rid.c_str());
    ret = clck.newowner;
    pthread_mutex_unlock(&clck.m);
    return rlock_protocol::RETRY;
  }

 process:
  // Assert either owner or future owner!
  assert(clck.amiowner || 
	 (clck.status() == cached_lock_rsm::ACQUIRING && 
	  clck.access == lock_protocol::WRITE));

  // New request for rid
  if (clck.requests[rid].xid < rxid) {
    tprintf("[%s] transfer_handler -> processing %llu, %d, %llu, %s\n", 
	    id.c_str(), lid, rtype, rxid,
	    rid.c_str());
    // Already gotten a WRITE request? Queue it to that mofo
    // Limits size of requests for invalidate
    std::map<std::string, request_t>::iterator it;
    for (it = clck.requests.begin(); it != clck.requests.end(); it++) {
      if ((it->second).type == lock_protocol::WRITE) {
        ret = (it->second).id;
        pthread_mutex_unlock(&clck.m);
        return rlock_protocol::RETRY;
      }
    }
    
    request_t t;
    t.id = rid;
    t.xid = rxid;
    t.type = (lock_protocol::lock_type) rtype;
    clck.requests[rid] = t;
    clck.xids[rid].cxid = rxid;

    if (rtype == lock_protocol::WRITE) {
      //clck.newowner = rid;
      //clck.amiowner = false;
      // Other readers (or readers in flight)
      if (clck.copyset.size() > 0 || clck.requests.size() > 1) {
        pthread_mutex_unlock(&clck.m);
        pthread_mutex_lock(&m);
        invalidateset[lid]++;
        pthread_cond_signal(&invalidater_cv); // At most one thread waiting
        pthread_mutex_unlock(&m);
        return rlock_protocol::OK;
      }
    }

    pthread_mutex_unlock(&clck.m);
    pthread_mutex_lock(&m);
    transferset[lid]++;
    pthread_cond_signal(&transferer_cv); // At most one thread waiting
    pthread_mutex_unlock(&m);
  }
  else {
    pthread_mutex_unlock(&clck.m);
    tprintf("[%s] transfer_handler -> failed %llu, %d, %s\n", 
	    id.c_str(), lid, rtype, 
	    rid.c_str());
  }

  return r;
}

clock_protocol::status
lock_client_cache_rsm::receive_handler(lock_protocol::lockid_t lid, 
				       lock_protocol::xid_t xid, 
				       std::string data, int &)
{
  tprintf("[%s] receive_handler -> %llu\n", 
	  id.c_str(), lid);
  int r = rlock_protocol::OK;
  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  tprintf("[%s] receive_handler -> %llu Got [%llu, %llu, %d]\n", \
	 id.c_str(), lid, clck.myxid.cxid, xid, clck.received);

  if (clck.myxid.cxid != xid || clck.received) { // could clck.received == 1?
    pthread_mutex_unlock(&clck.m);
    goto end;
  }

  tprintf("[%s] receive_handler -> %llu Processing {%d}...\n", id.c_str(), lid,
	  data.length());
  assert(clck.status() == cached_lock_rsm::ACQUIRING); // Must be in acquiring

  if (lu) {
    lu->dopopulate(lid, data);
  }

  if (clck.access == lock_protocol::WRITE) {
    clck.amiowner = true;
    clck.newowner = "";
  }
  else {
    assert(clck.requests.size() == 0); // no requests if reader
    clck.amiowner = false;
  }

  clck.copyset.clear();
  clck.received = true;
  clck.myxid.sxid = clck.myxid.cxid;
  pthread_cond_signal(&clck.receive_cv); // at most 1 thread waiting
  pthread_mutex_unlock(&clck.m);

 end:
  tprintf("[%s] receive_handler -> %llu Returning...\n", id.c_str(), lid);
  return r;
}
