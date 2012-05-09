// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

cachable_lock_rsm::cachable_lock_rsm()
{
  pthread_mutex_init(&m, NULL);
  nacquire = 0;
  owner = "";
}

cachable_lock_rsm::~cachable_lock_rsm() 
{
  pthread_mutex_destroy(&m);
}

static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoke_cv, NULL);
  pthread_cond_init(&retry_cv, NULL);
  rsm = _rsm;
  rsm->set_state_transfer(this);
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
}

lock_server_cache_rsm::~lock_server_cache_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoke_cv);
  pthread_cond_destroy(&retry_cv);
}

cachable_lock_rsm&
lock_server_cache_rsm::get_lock(lock_protocol::lockid_t lid) 
{
  ScopedLock ml(&m);
  return lockset[lid];
}

void
lock_server_cache_rsm::revoke(lock_protocol::lockid_t lid, 
			      cachable_lock_rsm &clck)
{
  printf("revoke %llu\n", lid);
  pthread_mutex_lock(&m);
  printf("revoke inserting %llu to revokeset\n", lid);
  revokeset.insert(lid);
  pthread_cond_broadcast(&revoke_cv);
  pthread_mutex_unlock(&m);
}

void
lock_server_cache_rsm::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while (true) {
    printf("revoker firing up!\n");
    // Get first revoke request
    pthread_mutex_lock(&m);
    while(revokeset.empty()) {
      printf("revoker sleeping! [%d]\n", revokeset.size());
      pthread_cond_wait(&revoke_cv, &m);
      printf("revoker waking up! [%d]\n", revokeset.size());
    }
    lock_protocol::lockid_t lid = *revokeset.begin();
    revokeset.erase(lid);
    pthread_mutex_unlock(&m);

    cachable_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    std::string clt = clck.owner;
    printf("revoker [%s] %llu\n", clt.c_str(), lid);
    bool revoke = clck.owner.length() > 0 && clck.waiters.size() > 0;
    pthread_mutex_unlock(&clck.m);

    if (!rsm->amiprimary() || !revoke) {
      continue;
    }

    printf("revoker sending revoke now [%s] %llu\n", clt.c_str(), lid);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      r = cl->call(rlock_protocol::revoke, lid, clck.seq[clt], ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      printf("revoker [%s] bind failed!\n", clt.c_str());
    }
  }
}

void
lock_server_cache_rsm::retryer()
{
  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.
  while (true) {
    printf("retryer firing up! [%d]\n", releasedset.size());
    pthread_mutex_lock(&m);
    while(releasedset.empty()) {
      printf("retryer sleeping! [%d]\n", releasedset.size());
      pthread_cond_wait(&retry_cv, &m);
      printf("retryer waking up! [%d]\n", releasedset.size());
    }
    lock_protocol::lockid_t lid = *releasedset.begin();
    releasedset.erase(lid);
    pthread_mutex_unlock(&m);

    printf("retryer next round! [%d]\n", releasedset.size());
    cachable_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    bool retry = clck.waiters.size() > 0;
    // Pick next client to 'retry'
    std::string clt = *clck.waiters.begin();
    printf("retryer [%s] %llu\n", clt.c_str(), lid);
    pthread_mutex_unlock(&clck.m);

    if (!rsm->amiprimary() || !retry) {
      continue;
    }

    printf("retryer sending retry now [%s] %llu\n", clt.c_str(), lid);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      r = cl->call(rlock_protocol::retry, lid, clck.seq[clt], ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      printf("retryer [%s] bind failed!\n", clt.c_str());
    }
  }
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
				   lock_protocol::xid_t xid, int &)
{
  lock_protocol::status r;
  cachable_lock_rsm &clck = get_lock(lid);

  pthread_mutex_lock(&clck.m);
  printf("acquire %llu [%s] W[%d]\n", lid, id.c_str(), clck.waiters.size()); 
  if (xid < clck.seq[id]) {
    r = lock_protocol::RPCERR;
  }
  else if (clck.owner == id && clck.seq[id] == xid) {
    printf("acquire %llu [%s] granting old request W[%d]\n", lid, id.c_str(),
	   clck.waiters.size());
    // If client resends request due to primary failure
    r = lock_protocol::OK;
    if (clck.waiters.size() > 0) {
      pthread_mutex_unlock(&clck.m);
      // More clients waiting so also send revoke
      revoke(lid, clck);
      goto end;
    }
  }
  else if (clck.owner.length() == 0) { 
    // No owner
    clck.owner = id;
    clck.seq[id] = xid;
    clck.waiters.erase(id);
    printf("acquire %llu [%s] granting lock W[%d]\n", lid, id.c_str(),
	   clck.waiters.size());
    r = lock_protocol::OK;
    if (clck.waiters.size() > 0) {
      // More clients waiting so also send revoke
      pthread_mutex_unlock(&clck.m);
      revoke(lid, clck);
      goto end;
    }
  }
  else {
    // Owned by some client already
    assert(clck.owner != id); // Owner shouldn't do acquire again
    clck.waiters.insert(id);
    printf("acquire %llu [%s] Adding to waiters W[%d]\n", lid, id.c_str(),
	   clck.waiters.size());
    clck.seq[id] = xid;
    r = lock_protocol::RETRY;
    pthread_mutex_unlock(&clck.m);
    revoke(lid, clck); 
    goto end;
  }
  pthread_mutex_unlock(&clck.m);
 end:
  return r;
}

int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
			       lock_protocol::xid_t xid, int &ret)
{
  lock_protocol::status r;
  cachable_lock_rsm &clck = get_lock(lid);

  pthread_mutex_lock(&clck.m);
  printf("release %llu [%s] W[%d]\n", lid, id.c_str(), clck.waiters.size());
  if (clck.seq[id] == xid) {
    printf("release %llu [%s] O[%s]\n", lid, id.c_str(), clck.owner.c_str());
    if (clck.owner == id) {
      clck.owner = "";
    }
    int size = clck.waiters.size();
    pthread_mutex_unlock(&clck.m);
    if (size > 0) { // Add to released set and wake up retryer
      pthread_mutex_lock(&m);
      releasedset.insert(lid);
      pthread_cond_broadcast(&retry_cv);
      pthread_mutex_unlock(&m);
    }
    r = lock_protocol::OK;
  }
  else {
    printf("release %llu [%s] out-of-date request!\n", lid, id.c_str());
    r = lock_protocol::RPCERR;
    pthread_mutex_unlock(&clck.m);
  }

  return r;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  ScopedLock ml(&m);
  marshall rep;

  std::map<lock_protocol::lockid_t, cachable_lock_rsm>::iterator lit;
  rep << (unsigned int) lockset.size();
  for (lit = lockset.begin(); lit != lockset.end(); lit++) {
    rep << lit->first;
    cachable_lock_rsm clck = lockset[lit->first];
    pthread_mutex_lock(&clck.m);
    rep << clck.owner;
    std::set<std::string>::iterator wit;
    rep << (unsigned int) clck.waiters.size();
    for (wit = clck.waiters.begin(); wit != clck.waiters.end(); wit++) {
      rep << *wit;
    }
    rep << clck.nacquire;
    std::map<std::string, lock_protocol::xid_t>::iterator sit;
    rep << (unsigned int) clck.seq.size();
    for (sit = clck.seq.begin(); sit != clck.seq.end(); sit++) {
      rep << sit->first;
      rep << sit->second;
    }
    pthread_mutex_unlock(&clck.m);
  }

  std::set<lock_protocol::lockid_t>::iterator rit;
  rep << (unsigned int) revokeset.size();
  for (rit = revokeset.begin(); rit != revokeset.end(); rit++) {
    rep << *rit;
  }
  rep << (unsigned int) releasedset.size();
  for (rit = releasedset.begin(); rit != releasedset.end(); rit++) {
    rep << *rit;
  }

  return rep.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  ScopedLock ml(&m);

  lockset.clear();
  revokeset.clear();
  releasedset.clear();

  unmarshall rep(state);
  unsigned int lsetsize;
  rep >> lsetsize;
  for (unsigned int i = 0; i < lsetsize; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    cachable_lock_rsm clck;
    rep >> clck.owner;
    unsigned int waitsize;
    rep >> waitsize;
    for (unsigned int i = 0; i < waitsize; i++) {
      std::string w;
      rep >> w;
      clck.waiters.insert(w);
    }
    rep >> clck.nacquire;
    unsigned int seqsize;
    rep >> seqsize;
    for (unsigned int i = 0; i < seqsize; i++) {
      std::string clt;
      lock_protocol::xid_t xid;
      rep >> clt;
      rep >> xid;
      clck.seq[clt] = xid;
    }
    lockset[lid] = clck;
  }

  unsigned int rsize;
  rep >> rsize;
  for (unsigned int i = 0; i < rsize; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    revokeset.insert(lid);
  }

  rep >> rsize;
  for (unsigned int i = 0; i < rsize; i++) {
    lock_protocol::lockid_t lid;
    rep >> lid;
    releasedset.insert(lid);
  }

  pthread_cond_broadcast(&revoke_cv);
  pthread_cond_broadcast(&retry_cv);
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = 0;
  return lock_protocol::OK;
}

