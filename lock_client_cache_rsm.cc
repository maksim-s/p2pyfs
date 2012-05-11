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
  pthread_cond_init(&retry_cv, NULL);
  pthread_cond_init(&receive_cv, NULL);
  pthread_cond_init(&readfree_cv, NULL);
  pthread_cond_init(&allfree_cv, NULL);
  pthread_cond_init(&none_cv, NULL);

  _status = NONE;
  retried = received = false;
  sxid = cxid = 0;
  otype = stype = lock_protocol::UNUSED;
}

cached_lock_rsm::~cached_lock_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&retry_cv);
  pthread_cond_destroy(&receive_cv);
  pthread_cond_destroy(&readfree_cv);
  pthread_cond_destroy(&allfree_cv);
  pthread_cond_destroy(&none_cv);
}

void cached_lock_rsm::set_status(lock_status s) 
{
  switch(s) {
  case NONE:
    stype = otype  = lock_protocol::UNUSED;
    pthread_cond_broadcast(&none_cv);
    break;
  case ACQUIRING:
    break;
  case REVOKING:
    break;
  case LOCKED:
    if (otype == lock_protocol::READ) {
      pthread_cond_broadcast(&readfree_cv);
    }
    break;
  case FREE:
    pthread_cond_broadcast(&allfree_cv);
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
ackthread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->acker();
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
  rlsrpc->reg(rlock_protocol::revoke, this, 
	      &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, 
  	      &lock_client_cache_rsm::retry_handler);
  rlsrpc->reg(rlock_protocol::transfer, this, 
	      &lock_client_cache_rsm::transfer_handler);
  rlsrpc->reg(clock_protocol::receive, this, 
	      &lock_client_cache_rsm::receive_handler);
  xid = 0;

  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoker_cv, NULL);
  pthread_cond_init(&transferer_cv, NULL);
  pthread_cond_init(&acker_cv, NULL);

  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);

  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &transferthread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &ackthread, (void *) this);
  VERIFY (r == 0);
}

lock_client_cache_rsm::~lock_client_cache_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoker_cv);
  pthread_cond_destroy(&transferer_cv);
  pthread_cond_destroy(&acker_cv);
}

void
lock_client_cache_rsm::revoker()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (revokeset.empty()) {
      pthread_cond_wait(&revoker_cv, &m);
    }
    lock_protocol::lockid_t lid;
    std::map<lock_protocol::lock_protocol::lockid_t, int>::iterator it;
    it = revokeset.begin();
    assert(it->second >= 1); // At least one revoke
    lid = it->first;
    revokeset.erase(it); // Remove from set
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    while(true) {
      if (clck.status() == cached_lock_rsm::ACQUIRING) {
	// The second acquire in flight MUST be WRITE
	assert(clck.stype == lock_protocol::WRITE);
	goto next;
      }

      if (clck.status() == cached_lock_rsm::FREE) {
	break;
      }

      // Wait till lock is FREE
      pthread_cond_wait(&clck.allfree_cv, &clck.m);
    }

    clck.set_status(cached_lock_rsm::REVOKING);

  next:
    assert(clck.owners.size() == 0 && clck.otype == lock_protocol::UNUSED);
    printf("[%s] releaser -> %llu\n", id.c_str(), lid);
    int r = srelease(lid, clck);
    assert(r == lock_protocol::OK); 

    // If not ACQUIRING, then set status to NONE
    if (clck.status() == cached_lock_rsm::REVOKING) {
      clck.set_status(cached_lock_rsm::NONE);
      pthread_cond_broadcast(&clck.none_cv);
    }
    pthread_mutex_unlock(&clck.m);
  }
}

void
lock_client_cache_rsm::transferer()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (transferset.empty()) {
      pthread_cond_wait(&transferer_cv, &m);
    }
    lock_protocol::lockid_t lid;
    std::map<lock_protocol::lock_protocol::lockid_t, 
      struct transfer_t>::iterator it;
    it = transferset.begin();
    struct transfer_t t = it->second;
    lid = it->first;
    transferset.erase(it); // Remove from set
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    while(true) {
      if (clck.status() == cached_lock_rsm::ACQUIRING) {
	// The second acquire in flight MUST be WRITE
	assert(clck.stype == lock_protocol::WRITE);
	goto next;
      }

      if (clck.status() == cached_lock_rsm::FREE) {
	break;
      }

      pthread_cond_wait(&clck.allfree_cv, &clck.m);
    }
    clck.set_status(cached_lock_rsm::REVOKING);

  next:
    assert(clck.owners.size() == 0 && clck.otype == lock_protocol::UNUSED);
    printf("[%s] transferer -> %llu, %s\n", id.c_str(), lid, t.rid.c_str());
    handle h(t.rid);
    rpcc *cl = h.safebind();
    if (cl) {
      int ret;
      std::string data = "";
      if (lu) {
	lu->dofetch(lid, data);
      }

      // Evict before sending? Won't work in failure case.
      if (t.rtype == lock_protocol::WRITE) {
	printf("[%s] transferer -> %llu revoking!\n", id.c_str(), lid);
	if (lu) {
	  lu->doevict(lid);
	}
      }
      
      printf("[%s] transferer -> %llu, %d\n", id.c_str(), lid, data.size());
      pthread_mutex_unlock(&clck.m);
      int r = cl->call(clock_protocol::receive, lid, t.rxid, data, ret);
      pthread_mutex_lock(&clck.m);
      assert(r == clock_protocol::OK);
    }
    else {
      printf("[%s] transferer -> %llu, %s failure!\n", id.c_str(), lid, 
	     t.rid.c_str());
    }

    if (t.rtype == lock_protocol::READ) {
      assert(clck.status() == cached_lock_rsm::REVOKING);
      clck.stype = lock_protocol::READ;
      clck.set_status(cached_lock_rsm::FREE);
    }
    // If not ACQUIRING, then set status to NONE
    else if (clck.status() == cached_lock_rsm::REVOKING) {
      clck.set_status(cached_lock_rsm::NONE);
      // remove from extent cache
      pthread_cond_broadcast(&clck.none_cv);
    }
    pthread_mutex_unlock(&clck.m);
  }
}

void
lock_client_cache_rsm::acker()
{
  while (true) {
    printf("[%s] acker -> Starting loop...\n", id.c_str());
    pthread_mutex_lock(&m);
    while (ackset.empty()) {
      printf("[%s] acker -> Wait empty...\n", id.c_str());
      pthread_cond_wait(&acker_cv, &m);
    }
    lock_protocol::lockid_t lid;
    std::map<lock_protocol::lock_protocol::lockid_t, int>::iterator it;
    it = ackset.begin();
    assert(it->second >= 1); // At least one receive
    lid = it->first;
    ackset.erase(it); // Remove from set
    pthread_mutex_unlock(&m);
    printf("[%s] acker -> %llu Preparing to send ack!\n", id.c_str(), lid);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    assert(clck.status() == cached_lock_rsm::ACQUIRING);

    printf("[%s] acker -> %llu\n", id.c_str(), lid);
    int r = sack(lid, clck);
    assert(r == lock_protocol::OK); 

    clck.received = true;
    pthread_cond_signal(&clck.receive_cv);
    pthread_mutex_unlock(&clck.m);
  }
}

cached_lock_rsm&
lock_client_cache_rsm::get_lock(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&m);
  return lockset[lid];
}

// Assumes mutex held for clck
lock_protocol::status
lock_client_cache_rsm::sacquire(lock_protocol::lockid_t lid,
				lock_protocol::lock_type type,
				cached_lock_rsm& clck)
{
  int ret, r;
  assert(clck.status() == cached_lock_rsm::ACQUIRING);
  clck.retried = false; // No retry message for server received for this call
  clck.received = false;
  clck.cxid = xid;
  printf("[%s] sacquire -> %llu, %d [%d, %llu]\n", id.c_str(), lid, 
	 type, clck.status(), xid);
  pthread_mutex_unlock(&clck.m);
  r = rsmc->call(lock_protocol::acquire, lid, id, 
		 (unsigned int) type, clck.cxid, ret);
  pthread_mutex_lock(&clck.m);
  return r;
}

// Assumes mutex held for clck
// Can be called by revoker only!
lock_protocol::status
lock_client_cache_rsm::srelease(lock_protocol::lockid_t lid, 
				cached_lock_rsm& clck)
{
  int ret, r;
  // Will have to call either in REVOKING or ACQUIRING w/ WRITE lock
  assert(clck.status() == cached_lock_rsm::REVOKING ||
	 (clck.status() == cached_lock_rsm::ACQUIRING && 
	  clck.stype == lock_protocol::WRITE));
  // No local guy should be owning the lock, however!
  assert(clck.otype == lock_protocol::UNUSED && clck.owners.size() == 0);
  printf("[%s] srelease -> %llu\n", id.c_str(), lid);
  if (lu) {
    lu->dorelease(lid);
  }
  pthread_mutex_unlock(&clck.m);
  // TODO cxid/sxid
  r = rsmc->call(lock_protocol::release, lid, id, clck.cxid, ret);
  pthread_mutex_lock(&clck.m);
  return r;
}

lock_protocol::status
lock_client_cache_rsm::sack(lock_protocol::lockid_t lid, 
			    cached_lock_rsm& clck)
{
  int ret, r;
  // Will have to call either in REVOKING or ACQUIRING w/ WRITE lock
  assert(clck.status() == cached_lock_rsm::ACQUIRING);
  // No local guy should be owning the lock, however!
  assert(clck.otype == lock_protocol::UNUSED && clck.owners.size() == 0);
  printf("[%s] sack -> %llu\n", id.c_str(), lid);
  pthread_mutex_unlock(&clck.m);
  r = rsmc->call(lock_protocol::ack, lid, id, clck.cxid, ret);
  pthread_mutex_lock(&clck.m);
  return r;
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

  printf("[%s] acquire -> %llu, %d\n", id.c_str(), lid, type);
  pthread_mutex_lock(&clck.m);

 start:
  switch(clck.status()) {
  case cached_lock_rsm::ACQUIRING:
    // Some other thread has already sent out request to server
    printf("[%s] acquire -> %llu, %d [ACQUIRING]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::ACQUIRING) {
      if (type == lock_protocol::READ) {
	pthread_cond_wait(&clck.readfree_cv, &clck.m);
      }
      else {
	// NOTE: We could actually be acquiring a READ lock from server,
	// but doesn't matter, will trigger new acquire flow once done.
	pthread_cond_wait(&clck.allfree_cv, &clck.m);
      }
    }
    goto start;
  case cached_lock_rsm::LOCKED:
    // Own lock, but someother thread using it.
    printf("[%s] acquire -> %llu, %d [LOCKED]\n", id.c_str(), lid, type);
    assert(clck.otype != lock_protocol::UNUSED);
    assert(clck.owners.size() > 0);

    while (clck.status() == cached_lock_rsm::LOCKED) {
      if (clck.otype == lock_protocol::READ && 
	  type == lock_protocol::READ) {
	clck.owners.insert(pthread_self());
	// Other read waiters can also join in the action!
	pthread_cond_broadcast(&clck.readfree_cv);
	r = lock_protocol::OK;
	break;
      }
      // Doesn't matter what stype is. Even if it is READ and we want WRITE,
      // we will trigger a new acquire lock once we loop back.
      pthread_cond_wait(&clck.allfree_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::FREE:
    // We own lock, and no one is using it.
    printf("[%s] acquire -> %llu, %d [FREE]\n", id.c_str(), lid, type);
    assert(clck.otype == lock_protocol::UNUSED);
    if (clck.stype == lock_protocol::WRITE || 
	(clck.stype == lock_protocol::READ && type == lock_protocol::READ)) {
      clck.owners.insert(pthread_self());
      clck.otype = type;
      clck.set_status(cached_lock_rsm::LOCKED);
      r = lock_protocol::OK;
      break;
    }
    else {
      goto sacquire; // Start acquire flow for WRITE
    }
  case cached_lock_rsm::REVOKING:
    // Release thread is releasing.
    printf("[%s] acquire -> %llu, %d [REVOKING]\n", id.c_str(), lid, type);
    while (clck.status() == cached_lock_rsm::REVOKING) {
      pthread_cond_wait(&clck.none_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::NONE:
    // Don't own lock, ask server to grant ownership.
    printf("[%s] acquire -> %llu, %d [NONE]\n", id.c_str(), lid, type);
  sacquire:
    clck.set_status(cached_lock_rsm::ACQUIRING);
    clck.stype = type;
    while ((r = sacquire(lid, type, clck)) == lock_protocol::RETRY) {
      printf("acquire NONE waiting for retry_cv\n");
      pthread_cond_wait(&clck.retry_cv, &clck.m);
      printf("acquire NONE waking up retry_cv\n");

    }
    clck.retried = true;
    assert(r == lock_protocol::OK);

    printf("[%s] acquire -> %llu, %d Got OK from ls\n", id.c_str(), lid, type);

    while (!clck.received) {
      printf("[%s] acquire -> %llu, %d Waiting for receive\n", 
	     id.c_str(), lid, type);
      pthread_cond_wait(&clck.receive_cv, &clck.m);
    }

    printf("[%s] acquire -> %llu, %d Got lock!\n", id.c_str(), lid, type);
    // Now we have lock + data!
    assert(clck.stype == type);
    assert(clck.owners.size() == 0);
    clck.otype = type;
    clck.owners.insert(pthread_self());
    clck.sxid = clck.cxid;
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

  printf("[%s] release -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

  if (!clck.owners.count(pthread_self())) {    
    printf("[%s] release -> %llu Non-owner!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }
  
  if (clck.status() != cached_lock_rsm::LOCKED) {
    printf("[%s] release -> %llu Not locked!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  clck.owners.erase(pthread_self());
  if (clck.owners.size() == 0) {
    clck.otype = lock_protocol::UNUSED;
    clck.set_status(cached_lock_rsm::FREE);
  }
  else {
    assert(clck.otype == lock_protocol::READ);
    pthread_cond_broadcast(&clck.readfree_cv);
  }

  r = lock_protocol::OK;

 end:
  pthread_mutex_unlock(&clck.m);
  return r;
}

rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
				      lock_protocol::xid_t xid, int &)
{
  int r = rlock_protocol::OK;

  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  // TODO: Check sxid/cxid!
  bool revoke = (clck.cxid == xid || clck.sxid == xid) 
    && clck.status() != cached_lock_rsm::NONE;
  pthread_mutex_unlock(&clck.m);

  if (revoke) {
    printf("[%s] revoke_handler -> %llu\n", id.c_str(), lid);
    pthread_mutex_lock(&m);
    revokeset[lid]++;
    pthread_cond_signal(&revoker_cv); // At most one thread waiting
    pthread_mutex_unlock(&m);
  }

  return r;
}

rlock_protocol::status
lock_client_cache_rsm::transfer_handler(lock_protocol::lockid_t lid, 
					lock_protocol::xid_t xid, 
					unsigned int rtype,
					std::string rid, 
					lock_protocol::xid_t rxid,
					int &)
{
  int r = rlock_protocol::OK;
  cached_lock_rsm &clck = get_lock(lid);
  // TODO: Check sxid/cxid!
  bool xfer = (clck.cxid == xid || clck.sxid == xid) 
    && clck.status() != cached_lock_rsm::NONE;
  pthread_mutex_unlock(&clck.m);

  if (xfer) {
    printf("[%s] transfer_handler -> %llu, %d, %s\n", id.c_str(), lid, rtype, 
	   rid.c_str());
    pthread_mutex_lock(&m);
    struct transfer_t t;
    t.rid = rid;
    t.rxid = rxid;
    t.rtype = (lock_protocol::lock_type) rtype;
    transferset[lid] = t;
    pthread_cond_signal(&transferer_cv); // At most one thread waiting
    pthread_mutex_unlock(&m);
  }

  pthread_mutex_unlock(&clck.m);
  return r;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
				     lock_protocol::xid_t xid, int &)
{
  int r = rlock_protocol::OK;

  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  // Check cxid!
  if (clck.cxid != xid || clck.retried) goto end;
  printf("[%s] retry_handler -> %llu\n", id.c_str(), lid);
  assert(clck.status() == cached_lock_rsm::ACQUIRING); // Must be in acquiring
  pthread_cond_signal(&clck.retry_cv); // At most one thread waiting
 end:
  pthread_mutex_unlock(&clck.m);
  return r;
}

clock_protocol::status
lock_client_cache_rsm::receive_handler(lock_protocol::lockid_t lid, 
				       lock_protocol::xid_t xid, 
				       std::string data, int &)
{
  int r = rlock_protocol::OK;
  printf("[%s] receive_handler -> %llu Getting clck\n", id.c_str(), lid);
  cached_lock_rsm &clck = get_lock(lid);
  printf("[%s] receive_handler -> %llu Acquiring clck.m\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);
  printf("[%s] receive_handler -> %llu Got clck.m [%llu, %llu, %d]\n", \
	 id.c_str(), lid, clck.cxid, xid, clck.received);
  // Check cxid!
  if (clck.cxid != xid || clck.received) {
    pthread_mutex_unlock(&clck.m);
    goto end;
  }
  printf("[%s] receive_handler -> %llu Processing %d...\n", id.c_str(), lid,
	 data.length());
  assert(clck.status() == cached_lock_rsm::ACQUIRING); // Must be in acquiring
  if (lu) {
    lu->dopopulate(lid, data);
  }
  pthread_mutex_unlock(&clck.m);

  pthread_mutex_lock(&m);
  ackset[lid]++;
  pthread_cond_signal(&acker_cv); // At most one thread waiting
  pthread_mutex_unlock(&m);

 end:
  printf("[%s] receive_handler -> %llu Returning...\n", id.c_str(), lid);
  return r;
}
