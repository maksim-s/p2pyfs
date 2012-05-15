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
  owner = "";
}

cachable_lock_rsm::~cachable_lock_rsm() 
{
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm, std::string _es) 
  : rsm (_rsm)
{
  pthread_mutex_init(&m, NULL);
  rsm = _rsm;
  es = _es;
  rsm->set_state_transfer(this); 
}

lock_server_cache_rsm::~lock_server_cache_rsm()
{
  pthread_mutex_destroy(&m);
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
				   unsigned int t, 
				   lock_protocol::xid_t xid, int &)
{
  // Can hold mutex across RPCs w/o any chance of remote deadlock as 
  // server only processes one type of request
  ScopedLock ml(&m);
  int r;
  std::string s = "";
  cachable_lock_rsm &clck = lockset[lid];

  tprintf("[%llu] Requester: %s, Owner: %s\n", lid, id.c_str(), 
	  clck.owner.c_str());

  if (clck.owner.length() == 0) {
    clck.owner = id;
    clck.xids[id] = xid;
    r = lock_protocol::XOK; // HACK: make this guy the owner
  }
  else {
    handle h(clck.owner);
    rpcc *cl = h.safebind();
    if (cl) {
    r = cl->call(rlock_protocol::transfer, lid, clck.xids[clck.owner], 
		 t, id, xid, s);
    }
    else {
      return lock_protocol::RPCERR;
    }
    
    if (r == rlock_protocol::RETRY) {
      clck.owner = s;
      r = lock_protocol::RETRY;
    }
    else {
      assert(r == rlock_protocol::OK);
      r = lock_protocol::OK;
    }
    
    clck.xids[id] = xid; // must update after call
    
    // if WRITE, update owner
    if ((lock_protocol::lock_type) t == lock_protocol::WRITE) {
      clck.owner = id;
    }
  }

  tprintf("[%llu] Requester: %s, Owner: %s, Returning: %d\n", lid, id.c_str(), 
	  clck.owner.c_str(), r);
  return r;
}

// NOT USED
int 
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
			       lock_protocol::xid_t xid, int &ret)
{
  return lock_protocol::OK;
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
