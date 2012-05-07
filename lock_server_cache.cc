// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

cachable_lock::cachable_lock()
{
  pthread_mutex_init(&m, NULL);
  revoking = false;
  nacquire = 0;
  owner = expected = "";
}

cachable_lock::~cachable_lock() 
{
  pthread_mutex_destroy(&m);
}

static void *
retrythread(void * ls)
{
  ((lock_server_cache *) ls)->retryer();
  return 0;
}

static void *
revokethread(void * ls)
{
  ((lock_server_cache *) ls)->revoker();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);

  // Retryer and Revoker Threads
  pthread_t retth, revth;
  int r;
  r = pthread_create(&retth, NULL, retrythread, (void *) this);
  assert(!r);
  r = pthread_create(&revth, NULL, revokethread, (void *) this);
  assert(!r);
}

lock_server_cache::~lock_server_cache()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoke_cv);
  pthread_cond_destroy(&retry_cv);
}

cachable_lock&
lock_server_cache::get_lock(lock_protocol::lockid_t lid) 
{
  ScopedLock ml(&m);
  return lockset[lid];
}

void
lock_server_cache::revoke(lock_protocol::lockid_t lid, cachable_lock &clck)
{
  if (!clck.revoking) {
    printf("revoke %llu\n", lid);
    clck.revoking = true;
    pthread_mutex_lock(&m);
    revokeset.insert(lid);
    pthread_cond_signal(&revoke_cv);
    pthread_mutex_unlock(&m);
  }
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status r;
  cachable_lock &clck = get_lock(lid);

  printf("acquire %llu [%s]\n", lid, id.c_str());
  pthread_mutex_lock(&clck.m);
  if (clck.owner.length() == 0) { 
    // No owner
    if ((clck.waiters.size() == 0 && clck.expected.length() == 0) ||
	clck.expected == id) {
      printf("acquire %llu [%s] Handing ownership\n", lid, id.c_str());
      // Expecting 'retry' from this client, or no one waiting for lock
      clck.owner = id;
      clck.expected = "";
      clck.revoking = false;
      r = lock_protocol::OK;
      if (clck.waiters.size() > 0) {
	// More clients waiting so also send revoke
	revoke(lid, clck);
      }
    }
    else {
      // Waiting for retry from expected or retryer to process lid
      printf("acquire %llu [%s] Adding to waiters\n", lid, id.c_str());
      clck.waiters.push_back(id);
      r = lock_protocol::RETRY;
      revoke(lid, clck);
    }
  }
  else {
    // Owned by some client already
    assert(clck.owner != id); // Owner shouldn't do acquire again
    printf("acquire %llu [%s] Adding to waiters\n", lid, id.c_str());
    clck.waiters.push_back(id);
    r = lock_protocol::RETRY;
    revoke(lid, clck);
  }
  pthread_mutex_unlock(&clck.m);
  return r;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &ret)
{
  lock_protocol::status r;
  cachable_lock &clck = get_lock(lid);

  printf("release %llu [%s]\n", lid, id.c_str());
  pthread_mutex_lock(&clck.m);
  if (clck.owner != id) {
    printf("release %llu [%s] Not owner!\n", lid, id.c_str());
    r = lock_protocol::NOENT;
  }
  else {
    clck.owner = "";
    assert(clck.expected.length() == 0); // Only set by retryer thread!
    if (clck.waiters.size() > 0) {
      releasedset.insert(lid);
      pthread_cond_signal(&retry_cv);
    }
    r = lock_protocol::OK;
  }
  pthread_mutex_unlock(&clck.m);
  return r;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = 0;
  return lock_protocol::OK;
}

void
lock_server_cache::retryer()
{
  while (true) {
    pthread_mutex_lock(&m);
    while(releasedset.empty()) {
      pthread_cond_wait(&retry_cv, &m);
    }
    lock_protocol::lockid_t lid = *releasedset.begin();
    releasedset.erase(lid);
    pthread_mutex_unlock(&m);

    cachable_lock &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    assert(clck.owner.length() == 0);
    assert(clck.expected.length() == 0);
    assert(clck.waiters.size() > 0);

    // Pick next client to 'retry'
    std::string clt = clck.waiters.front();
    clck.waiters.pop_front();
    clck.expected = clt;
    pthread_mutex_unlock(&clck.m);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      printf("retryer [%s] %llu\n", clt.c_str(), lid);
      r = cl->call(rlock_protocol::retry, lid, ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      printf("retryer [%s] bind failed!\n", clt.c_str());
    }
  }
}

void
lock_server_cache::revoker()
{
  while (true) {
    // Get first revoke request
    pthread_mutex_lock(&m);
    while(revokeset.empty()) {
      pthread_cond_wait(&revoke_cv, &m);
    }
    lock_protocol::lockid_t lid = *revokeset.begin();
    pthread_mutex_unlock(&m);

    // Set state to revoking = true
    cachable_lock &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    assert(clck.owner.length() > 0);
    assert(clck.expected.length() == 0);
    assert(clck.waiters.size() > 0);
    clck.revoking = true;
    std::string clt = clck.owner;
    pthread_mutex_unlock(&clck.m);

    // Remove from revokeset
    pthread_mutex_lock(&m);
    revokeset.erase(lid);
    pthread_mutex_unlock(&m);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      printf("revoker [%s] %llu\n", clt.c_str(), lid);
      r = cl->call(rlock_protocol::revoke, lid, ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      printf("revoker [%s] bind failed!\n", clt.c_str());
    }
  }
}
