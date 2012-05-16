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
  rif = ltype = access = lock_protocol::UNUSED;
  amiowner = false;
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

  }
}

// must process READ clck.requests before WRITEs
void
lock_client_cache_rsm::transferer()
{
  while (true) {

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
    assert(clck.rif == lock_protocol::UNUSED); // no req in flight
    clck.rif = type;
    int ret;

    clck.acquired = false;
    clck.received = false;
    tprintf("[%s] sacquire -> %llu, %d [%d]\n", id.c_str(), lid, 
	  type, clck.status());

    pthread_mutex_unlock(&clck.m);
    r = rsmc->call(lock_protocol::acquire, lid, id, 
		   (unsigned int) type, 0, ret);
    pthread_mutex_lock(&clck.m);

    assert(r == lock_protocol::OK);

    tprintf("[%s] acquire -> %llu, %d Got OK from LS {%d}\n", 
	    id.c_str(), lid, type, clck.received);

    while (!clck.received) {
      tprintf("[%s] acquire -> %llu, %d Waiting for receive\n", 
	      id.c_str(), lid, type);
      pthread_cond_wait(&clck.receive_cv, &clck.m);
    }

    tprintf("[%s] acquire -> %llu, %d Got lock + data!\n", 
	    id.c_str(), lid, type);

    assert(clck.access == type);
    assert(clck.lowners.size() == 0);

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
  return clock_protocol::OK;
}

// Owner -> Reader
clock_protocol::status
lock_client_cache_rsm::invalidate_handler(lock_protocol::lockid_t lid, 
                                      std::string cid, int &)
{
  return clock_protocol::OK;
}

rlock_protocol::status
lock_client_cache_rsm::transfer_handler(lock_protocol::lockid_t lid, 
					unsigned int rtype,
					std::string rid, 
					int &)
{
  int r = rlock_protocol::OK;
  return r;
}

clock_protocol::status
lock_client_cache_rsm::receive_handler(lock_protocol::lockid_t lid, 
				       std::string data, int &)
{
  int r = rlock_protocol::OK;
  return r;
}
