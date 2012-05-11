// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

cached_lock::cached_lock()
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&retry_cv, NULL);
  pthread_cond_init(&free_cv, NULL);
  pthread_cond_init(&release_cv, NULL);

  _status = NONE;
  owner = -1;
  retry = false;
}

cached_lock::~cached_lock()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&retry_cv);
  pthread_cond_destroy(&free_cv);
  pthread_cond_destroy(&release_cv);
}

void cached_lock::set_status(lock_status s) 
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

cached_lock::lock_status cached_lock::status()
{
  return _status;
}

static void *
rthread(void *lc)
{
  ((lock_client_cache *) lc)->releaser();
  return 0;
}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();

  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&release_cv, NULL);

  // Releaser Thread
  pthread_t rth;
  int r = pthread_create(&rth, NULL, rthread, (void *) this);
  assert(!r);
}

lock_client_cache::~lock_client_cache()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&release_cv);
}

void
lock_client_cache::releaser()
{
  while (true) {
    pthread_mutex_lock(&m);
    while (releaseset.empty()) {
      pthread_cond_wait(&release_cv, &m);
    }
    lock_protocol::lockid_t lid;
    std::map<lock_protocol::lock_protocol::lockid_t, int>::iterator it;
    it = releaseset.begin();
    assert(it->second == 1); // At most one revoke
    lid = it->first;
    releaseset.erase(it); // Remove from set, wont receive another revoke!
    pthread_mutex_unlock(&m);

    cached_lock &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    while (clck.status() != cached_lock::FREE) {
      // Wait till lock is FREE
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    printf("[%s] releaser -> %llu [%d]\n", id.c_str(), lid, 
	   clck.waiters.size());
    // Release lock at server
    clck.set_status(cached_lock::RELEASING);
    int r = srelease(lid, clck);
    // Lock successfully released
    assert(r == lock_protocol::OK);
    clck.set_status(cached_lock::NONE);
    pthread_cond_broadcast(&clck.release_cv);
    pthread_mutex_unlock(&clck.m);
  }
}

cached_lock&
lock_client_cache::get_lock(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&m);
  return lockset[lid];
}

// Assumes mutex held for cl
lock_protocol::status
lock_client_cache::sacquire(lock_protocol::lockid_t lid, 
			    lock_protocol::lock_type type,cached_lock& clck)
{
  int ret, r;
  assert(clck.status() == cached_lock::ACQUIRING);

  printf("[%s] sacquire -> %llu\n", id.c_str(), lid);
  clck.retry = false; // No retry message for server received for this call
  pthread_mutex_unlock(&clck.m);
  r = cl->call(lock_protocol::acquire, lid, id, ret);
  pthread_mutex_lock(&clck.m);
  return r;
}

// Assumes mutex held for cl
lock_protocol::status
lock_client_cache::srelease(lock_protocol::lockid_t lid, cached_lock& clck)
{
  int ret, r;
  assert(clck.status() == cached_lock::RELEASING);

  printf("[%s] srelease -> %llu\n", id.c_str(), lid);
  clck.retry = false;
  clck.owner = -1;
  pthread_mutex_unlock(&clck.m);
  lu->dorelease(lid);
  r = cl->call(lock_protocol::release, lid, id, ret);
  pthread_mutex_lock(&clck.m);
  // Since we own lock, should always return OK
  assert(r == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid,
			   lock_protocol::lock_type type)
{
  int r;
  cached_lock &clck = get_lock(lid);

  printf("[%s] acquire -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

 start:
  clck.waiters.insert(pthread_self()); // Add to waiters

  switch(clck.status()) {
  case cached_lock::ACQUIRING:
    // Some other thread has already sent out request to server
    printf("[%s] acquire -> %llu [ACQUIRING]\n", id.c_str(), lid);
    while (clck.status() == cached_lock::ACQUIRING) {
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock::LOCKED:
    // Own lock, but someother thread using it.
    printf("[%s] acquire -> %llu [LOCKED]\n", id.c_str(), lid);
    if (clck.owner == pthread_self()) {
      printf("[%s] acquire -> %llu [LOCKED] Re-entrant!\n", id.c_str(), lid);
      r = lock_protocol::OK;
      break;
    }
    while (clck.status() == cached_lock::LOCKED) {
      pthread_cond_wait(&clck.free_cv, &clck.m);
    }
    goto start;
  case cached_lock::FREE:
    // We own lock, and no one is using it.
    printf("[%s] acquire -> %llu [FREE]\n", id.c_str(), lid);
    clck.set_status(cached_lock::LOCKED);
    r = lock_protocol::OK;
    break;
  case cached_lock::RELEASING:
    // Release thread is releasing.
    printf("[%s] acquire -> %llu [RELEASING]\n", id.c_str(), lid);
    while (clck.status() == cached_lock::RELEASING) {
      pthread_cond_wait(&clck.release_cv, &clck.m);
    }
    goto start;
  case cached_lock::NONE:
    // Don't own lock, ask server to grant ownership.
    printf("[%s] acquire -> %llu [NONE]\n", id.c_str(), lid);
    clck.set_status(cached_lock::ACQUIRING);
    while ((r = sacquire(lid, lock_protocol::WRITE, clck)) == 
	   lock_protocol::RETRY) {
      while (!clck.retry) {
	pthread_cond_wait(&clck.retry_cv, &clck.m);
      }
    }
    // We now have ownership of lock
    assert(r == lock_protocol::OK);
    clck.set_status(cached_lock::LOCKED);
    break;
  default:
    break;
  }

  pthread_mutex_unlock(&clck.m);
  return r;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int r;
  cached_lock &clck = get_lock(lid);

  printf("[%s] release -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);

  if (pthread_self() != clck.owner) {
    printf("[%s] release -> %llu Non-owner!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  if (clck.status() != cached_lock::LOCKED) {
    printf("[%s] release -> %llu Not locked!\n", id.c_str(), lid);
    r = lock_protocol::NOENT;
    goto end;
  }

  clck.set_status(cached_lock::FREE);

 end:
  pthread_mutex_unlock(&clck.m);
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int r = rlock_protocol::OK;

  printf("[%s] revoke_handler -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&m);
  releaseset[lid]++;
  pthread_cond_signal(&release_cv); // At most one thread waiting
  pthread_mutex_unlock(&m);

  return r;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int r = rlock_protocol::OK;
  cached_lock &clck = get_lock(lid);

  printf("[%s] retry_handler -> %llu\n", id.c_str(), lid);
  pthread_mutex_lock(&clck.m);
  assert(clck.status() == cached_lock::ACQUIRING); // Must be in acquiring
  clck.retry = true;
  pthread_cond_signal(&clck.retry_cv); // At most one thread waiting
  pthread_mutex_unlock(&clck.m);

  return r;
}



