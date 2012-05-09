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

cached_lock_rsm::cached_lock_rsm()
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&retry_cv, NULL);
  pthread_cond_init(&free_cv, NULL);
  pthread_cond_init(&release_cv, NULL);

  _status = NONE;
  owner = -1;
  retry = false;
  seq = 0;
}

cached_lock_rsm::~cached_lock_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&retry_cv);
  pthread_cond_destroy(&free_cv);
  pthread_cond_destroy(&release_cv);
}

void cached_lock_rsm::set_status(lock_status s) 
{
  switch(s) {
  case NONE:
  case ACQUIRING:
  case RELEASING:
    owner = -1;
    break;
  case LOCKED:
    waiters.erase(pthread_self()); // No longer waiting
    owner = pthread_self();
    break;
  case FREE:
    owner = -1;
    pthread_cond_broadcast(&free_cv);
    break;
  }
  _status = s;
}

cached_lock_rsm::lock_status cached_lock_rsm::status()
{
  return _status;
}

static void *
releasethread(void *x)
{
  lock_client_cache_rsm *cc = (lock_client_cache_rsm *) x;
  cc->releaser();
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
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);
  xid = 0;

  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&release_cv, NULL);

  // You fill this in Step Two, Lab 7
  // - Create rsmc, and use the object to do RPC 
  //   calls instead of the rpcc object of lock_client
  rsmc = new rsm_client(xdst);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  VERIFY (r == 0);
}

lock_client_cache_rsm::~lock_client_cache_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&release_cv);
}

void
lock_client_cache_rsm::releaser()
{
  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  while (true) {
    pthread_mutex_lock(&m);
    while (releaseset.empty()) {
      pthread_cond_wait(&release_cv, &m);
      printf("[%s] releaser waking up [%d]\n", id.c_str(), releaseset.size());
    }
    lock_protocol::lockid_t lid;
    std::map<lock_protocol::lock_protocol::lockid_t, int>::iterator it;
    it = releaseset.begin();
    assert(it->second >= 1); // At least one revoke
    lid = it->first;
    releaseset.erase(it); // Remove from set, wont receive another revoke!
    pthread_mutex_unlock(&m);

    cached_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);

    // Must do this check here, because despite code in revoke_handler
    // I think there's is a chance two revokerequest sneak in for same lid
    // and same xid
    if (clck.status() == cached_lock_rsm::NONE) {
      pthread_mutex_unlock(&clck.m);
      continue;
    }

    while (clck.status() != cached_lock_rsm::FREE) {
      // Wait till lock is FREE
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }

    printf("[%s] releaser -> %llu [%d]\n", id.c_str(), lid, 
	   clck.waiters.size());
    // Release lock at server
    clck.set_status(cached_lock_rsm::RELEASING);
    int r = srelease(lid, clck);
    // Lock successfully released
    assert(r == lock_protocol::OK); 
    clck.set_status(cached_lock_rsm::NONE);
    pthread_cond_broadcast(&clck.release_cv);
    pthread_mutex_unlock(&clck.m);
  }
}

cached_lock_rsm&
lock_client_cache_rsm::get_lock(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&m);
  return lockset[lid];
}

// Assumes mutex held for cl
lock_protocol::status
lock_client_cache_rsm::sacquire(lock_protocol::lockid_t lid, 
				cached_lock_rsm& clck)
{
  int ret, r;
  printf("[%s] sacquire -> %llu [%d]\n", id.c_str(), lid, clck.status());
  assert(clck.status() == cached_lock_rsm::ACQUIRING);
  clck.retry = false; // No retry message for server received for this call
  clck.seq = xid;
  pthread_mutex_unlock(&clck.m);
  r = rsmc->call(lock_protocol::acquire, lid, id, clck.seq, ret);
  pthread_mutex_lock(&clck.m);
  return r;
}

// Assumes mutex held for cl
lock_protocol::status
lock_client_cache_rsm::srelease(lock_protocol::lockid_t lid, 
				cached_lock_rsm& clck)
{
  int ret, r;
  assert(clck.status() == cached_lock_rsm::RELEASING);

  printf("[%s] srelease -> %llu\n", id.c_str(), lid);
  clck.owner = -1;
  pthread_mutex_unlock(&clck.m);
  if (lu) {
    lu->dorelease(lid);
  }
  r = rsmc->call(lock_protocol::release, lid, id, clck.seq, ret);
  pthread_mutex_lock(&clck.m);
  return r;
}

lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  int r;

  pthread_mutex_lock(&m);
  xid++; // xid must be globally protected
  pthread_mutex_unlock(&m);

  cached_lock_rsm &clck = get_lock(lid);

  printf("[%s] acquire -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

 start:
  clck.waiters.insert(pthread_self()); // Add to waiters

  switch(clck.status()) {
  case cached_lock_rsm::ACQUIRING:
    // Some other thread has already sent out request to server
    printf("[%s] acquire -> %llu [ACQUIRING]\n", id.c_str(), lid);
    while (clck.status() == cached_lock_rsm::ACQUIRING) {
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::LOCKED:
    // Own lock, but someother thread using it.
    printf("[%s] acquire -> %llu [LOCKED]\n", id.c_str(), lid);
    if (clck.owner == pthread_self()) {
      printf("[%s] acquire -> %llu [LOCKED] Re-entrant!\n", id.c_str(), lid);
      r = lock_protocol::OK;
      break;
    }
    while (clck.status() == cached_lock_rsm::LOCKED) {
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::FREE:
    // We own lock, and no one is using it.
    printf("[%s] acquire -> %llu [FREE]\n", id.c_str(), lid);
    clck.set_status(cached_lock_rsm::LOCKED);
    r = lock_protocol::OK;
    break;
  case cached_lock_rsm::RELEASING:
    // Release thread is releasing.
    printf("[%s] acquire -> %llu [RELEASING]\n", id.c_str(), lid);
    while (clck.status() == cached_lock_rsm::RELEASING) {
      pthread_cond_wait(&clck.release_cv, &clck.m);
    }
    goto start;
  case cached_lock_rsm::NONE:
    // Don't own lock, ask server to grant ownership.
    printf("[%s] acquire -> %llu [NONE]\n", id.c_str(), lid);
    clck.set_status(cached_lock_rsm::ACQUIRING);
    while ((r = sacquire(lid, clck)) == lock_protocol::RETRY) {
      struct timeval tp;
      gettimeofday(&tp, NULL);
      struct timespec ts;
      ts.tv_sec  = tp.tv_sec;
      ts.tv_nsec = tp.tv_usec * 1000;
      ts.tv_sec += 3;
      pthread_cond_timedwait(&clck.retry_cv, &clck.m, &ts);
    }
    clck.retry = true;
    // We now have ownership of lock
    assert(r == lock_protocol::OK); //TODO
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

  if (pthread_self() != clck.owner) {
    printf("[%s] release -> %llu Non-owner!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  if (clck.status() != cached_lock_rsm::LOCKED) {
    printf("[%s] release -> %llu Not locked!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  clck.set_status(cached_lock_rsm::FREE);

 end:
  pthread_mutex_unlock(&clck.m);
  return lock_protocol::OK;
}


rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, 
			          lock_protocol::xid_t xid, int &)
{
  int r = rlock_protocol::OK;

  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  bool revoke = clck.seq == xid && clck.status() != cached_lock_rsm::NONE;
  pthread_mutex_unlock(&clck.m);

  if (revoke) {
    printf("[%s] revoke_handler -> %llu\n", id.c_str(), lid);
    pthread_mutex_lock(&m);
    releaseset[lid]++;
    pthread_cond_signal(&release_cv); // At most one thread waiting
    pthread_mutex_unlock(&m);
    printf("[%s] revoke_handler complete -> %llu\n", id.c_str(), lid);
  }

  return r;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, 
			         lock_protocol::xid_t xid, int &)
{
  int r = rlock_protocol::OK;

  cached_lock_rsm &clck = get_lock(lid);
  pthread_mutex_lock(&clck.m);
  if (clck.seq != xid || clck.retry) goto end;
  printf("[%s] retry_handler -> %llu\n", id.c_str(), lid);
  assert(clck.status() == cached_lock_rsm::ACQUIRING); // Must be in acquiring
  pthread_cond_signal(&clck.retry_cv); // At most one thread waiting
 end:
  pthread_mutex_unlock(&clck.m);
  return r;
}
