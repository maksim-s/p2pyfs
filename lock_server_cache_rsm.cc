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
  owner = fowner = "";
  type = ftype = lock_protocol::UNUSED;
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
transferthread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->transferer();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm, std::string _es) 
  : rsm (_rsm)
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&revoker_cv, NULL);
  pthread_cond_init(&transferer_cv, NULL);
  pthread_cond_init(&retryer_cv, NULL);
  rsm = _rsm;
  es = _es;
  rsm->set_state_transfer(this);
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &transferthread, (void *) this);
  VERIFY (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  VERIFY (r == 0);
  
}

lock_server_cache_rsm::~lock_server_cache_rsm()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&revoker_cv);
  pthread_cond_destroy(&transferer_cv);
  pthread_cond_destroy(&retryer_cv);
}

cachable_lock_rsm&
lock_server_cache_rsm::get_lock(lock_protocol::lockid_t lid) 
{
  ScopedLock ml(&m);
  return lockset[lid];
}

void
lock_server_cache_rsm::revoker()
{
  while (true) {
    // Get first revoke request
    pthread_mutex_lock(&m);
    while(revokeset.empty()) {
      pthread_cond_wait(&revoker_cv, &m);
    }
    lock_protocol::lockid_t lid = *revokeset.begin();
    revokeset.erase(lid);
    pthread_mutex_unlock(&m);

    cachable_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    tprintf("revoker [%s] %llu {%s, %d}\n", clck.owner.c_str(), lid,
	    clck.fowner.c_str(), clck.ftype);
    bool revoke = clck.fowner.length() > 0 
      && clck.ftype == lock_protocol::WRITE;
    std::set<std::string> copyset = clck.copyset;

    tprintf("revoker [%s] %llu {%s, %d} [revoke %d] (size %d)\n", 
	    clck.owner.c_str(), lid,
	    clck.fowner.c_str(), clck.ftype,
	    revoke, clck.copyset.size());
    pthread_mutex_unlock(&clck.m);

    if (!rsm->amiprimary() || !revoke) {
      continue;
    }

    std::set<std::string>::iterator it;
    for(it = copyset.begin(); it != copyset.end(); it++) {
      pthread_mutex_lock(&clck.m);
      lock_protocol::xid_t xid = clck.xids[*it].sxid;
      pthread_mutex_unlock(&clck.m);

      handle h(*it);
      rpcc *cl = h.safebind();
      int r, ret;
      if (cl) {
	r = cl->call(rlock_protocol::revoke, lid, xid, ret);
	assert(r == rlock_protocol::OK);
      }
      else {
	tprintf("revoker [%s] bind failed!\n", (*it).c_str());
      }
    }
    pthread_mutex_lock(&clck.m);
    assert(copyset.size() >= clck.copyset.size());
    for (it = clck.copyset.begin(); it != clck.copyset.end(); it++) {
      assert(copyset.count(*it) == 1);
    }
    pthread_mutex_unlock(&clck.m);
  }
}

void
lock_server_cache_rsm::transferer()
{
  while(true) {
    pthread_mutex_lock(&m);
    while(transferset.empty()) {
      pthread_cond_wait(&transferer_cv, &m);
    }
    lock_protocol::lockid_t lid = *transferset.begin();
    transferset.erase(lid);
    pthread_mutex_unlock(&m);
    cachable_lock_rsm &clck = get_lock(lid);
    bool transfer = clck.fowner.length() > 0;
    pthread_mutex_lock(&clck.m);
    std::string clt;
    if (clck.owner.length() == 0) {
      clt = es;
    } else {
      clt = clck.owner;
    }
    tprintf("transferer [%s] %llu\n", clt.c_str(), lid);
    lock_protocol::xid_t xid = clck.xids[clt].sxid;
    std::string rid = clck.fowner;
    lock_protocol::xid_t rxid = clck.xids[rid].cxid;
    unsigned int rtype = (unsigned int) clck.ftype;
    pthread_mutex_unlock(&clck.m);

    if (!rsm->amiprimary() || !transfer) {
      continue;
    }

    tprintf("transferer sending transfer now [%s] %llu\n", clt.c_str(), lid);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      std::string fowner = clck.fowner;
      r = cl->call(rlock_protocol::transfer, lid, xid,
		   rtype, rid, rxid, ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      tprintf("transferer [%s] bind failed!\n", clt.c_str());
    }
    tprintf("transferer sent to [%s] %llu\n", clt.c_str(), lid);
  }
}

void
lock_server_cache_rsm::retryer()
{
  while (true) {
    pthread_mutex_lock(&m);
    while(retryset.empty()) {
      pthread_cond_wait(&retryer_cv, &m);
    }
    lock_protocol::lockid_t lid = *retryset.begin();
    retryset.erase(lid);
    pthread_mutex_unlock(&m);
    cachable_lock_rsm &clck = get_lock(lid);
    pthread_mutex_lock(&clck.m);
    bool retry = clck.waiters.size() > 0;
    // Pick next client to 'retry'
    std::string clt = *clck.waiters.begin();
    clck.waiters.erase(clt); // remove from waiters
    lock_protocol::xid_t xid = clck.xids[clt].cxid;
    tprintf("retryer [%s] %llu\n", clt.c_str(), lid);
    pthread_mutex_unlock(&clck.m);

    if (!rsm->amiprimary() || !retry) {
      continue;
    }

    tprintf("retryer sending retry now [%s] %llu\n", clt.c_str(), lid);

    handle h(clt);
    rpcc *cl = h.safebind();
    int r, ret;
    if (cl) {
      r = cl->call(rlock_protocol::retry, lid, xid, ret);
      assert(r == rlock_protocol::OK);
    }
    else {
      tprintf("retryer [%s] bind failed!\n", clt.c_str());
    }
  }
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
				   unsigned int t, 
				   lock_protocol::xid_t xid, int &)
{
  lock_protocol::status r;
  lock_protocol::lock_type type = (lock_protocol::lock_type) t;
  assert(type != lock_protocol::UNUSED);
  cachable_lock_rsm &clck = get_lock(lid);

  pthread_mutex_lock(&clck.m);
  tprintf("acquire %llu, %d [%s] [%llu, (%llu, %llu)]\n", lid, t, id.c_str(), 
	 xid, clck.xids[id].cxid, clck.xids[id].sxid); 
  if (xid < clck.xids[id].cxid) {
    r = lock_protocol::RPCERR;
    pthread_mutex_unlock(&clck.m);
    goto end;
  }
  
  if (xid == clck.xids[id].cxid) {
    if (type == clck.type) {
      if (type == lock_protocol::WRITE) {
	assert(clck.copyset.size() == 0);
      }

      if (clck.owner == id || clck.copyset.count(id) == 1) {
	r = lock_protocol::OK;
	if (clck.waiters.size() > 0) {
	  pthread_mutex_unlock(&clck.m);
	  pthread_mutex_lock(&m);
	  retryset.insert(lid);
	  pthread_cond_signal(&retryer_cv);
	  pthread_mutex_unlock(&m);
	}
	else {
	  pthread_mutex_unlock(&clck.m);
	}
	goto end;
      }
    }
  }

  clck.xids[id].cxid = xid;

  if (clck.fowner.length() != 0) {
    tprintf("acquire %s adding to waiters...\n", id.c_str());
    clck.waiters.insert(id);
    r = lock_protocol::RETRY;
    pthread_mutex_unlock(&clck.m);
    goto end;    
  }

  // No owner or fowner!
  if (clck.owner.length() == 0) {
    tprintf("acquire %s adding to fowner w/ no owner...\n", id.c_str());
    assert(clck.copyset.size() == 0);
    clck.fowner = id;
    clck.ftype = type;
    r = lock_protocol::OK;
    pthread_mutex_unlock(&clck.m);
    // transferer will figure out that need to send from ES
    pthread_mutex_lock(&m);
    transferset.insert(lid); 
    pthread_cond_signal(&transferer_cv);
    pthread_mutex_unlock(&m);
    goto end;
  }

  tprintf("acquire %s adding to fowner with owner...\n", id.c_str());
  clck.fowner = id;
  clck.ftype = type;
  r = lock_protocol::OK;
  pthread_mutex_unlock(&clck.m);

  pthread_mutex_lock(&m);
  if (type == lock_protocol::WRITE) {
    // revoker will invalidate all copyset items
    tprintf("acquire %s I am write, revoke...\n", id.c_str());
    if (clck.copyset.size() > 0) {
      revokeset.insert(lid);
      pthread_cond_signal(&revoker_cv);
    }
    else { // copyset empty? straight to xfer flow!
      transferset.insert(lid);
      pthread_cond_signal(&transferer_cv);
    }
  }
  else {
    // transferer will figure out that need to send from owner
    tprintf("acquire %s I am read, xfer...\n", id.c_str());
    transferset.insert(lid);
    pthread_cond_signal(&transferer_cv);
  }
  pthread_mutex_unlock(&m);

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
  tprintf("release %llu [%s] W[%d]\n", lid, id.c_str(), clck.waiters.size());
  if (clck.xids[id].sxid == xid) {
    assert(clck.owner != id); // owner never sends release; its implicit in ack
    clck.copyset.erase(id);
    int size = clck.copyset.size();
    pthread_mutex_unlock(&clck.m);

    if (size == 0) { // Invalidated all copset, add to transferer queue
      pthread_mutex_lock(&m);
      transferset.insert(lid);
      pthread_cond_broadcast(&transferer_cv);
      pthread_mutex_unlock(&m);
    }
    r = lock_protocol::OK;
  }
  else {
    tprintf("release %llu [%s] out-of-date request!\n", lid, id.c_str());
    r = lock_protocol::RPCERR;
    pthread_mutex_unlock(&clck.m);
  }

  return r;
}

int 
lock_server_cache_rsm::ack(lock_protocol::lockid_t lid, std::string id, 
			       lock_protocol::xid_t xid, int &ret)
{
  lock_protocol::status r;
  cachable_lock_rsm &clck = get_lock(lid);

  pthread_mutex_lock(&clck.m);
  tprintf("ack %llu [%s] [%llu, (%llu, %llu)]\n", lid, id.c_str(), xid, 
	 clck.xids[id].cxid, clck.xids[id].sxid);
  if (clck.xids[id].cxid == xid) {
    clck.xids[id].sxid = xid;
    if (clck.owner.length() == 0) {
      assert(clck.copyset.size() == 0);
      clck.owner = id;
    }
    else if (clck.ftype == lock_protocol::READ) {
      clck.copyset.insert(id);
    }
    else {
      assert(clck.copyset.size() == 0);
      clck.owner = id;
    }

    clck.type = clck.ftype;
    clck.ftype = lock_protocol::UNUSED;
    clck.fowner = "";
    int size = clck.waiters.size();
    pthread_mutex_unlock(&clck.m);

    if (size > 0) { // Process next waiter in line
      pthread_mutex_lock(&m);
      retryset.insert(lid);
      pthread_cond_broadcast(&retryer_cv);
      pthread_mutex_unlock(&m);
    }
    r = lock_protocol::OK;
  }
  else {
    tprintf("ack %llu [%s] out-of-date request!\n", lid, id.c_str());
    r = lock_protocol::RPCERR;
    pthread_mutex_unlock(&clck.m);
  }

  return r;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  ScopedLock ml(&m);
  return "";
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  ScopedLock ml(&m);
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = 0;
  return lock_protocol::OK;
}
