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
  owner = "";
}

cachable_lock_rsm::~cachable_lock_rsm() 
{
  pthread_mutex_destroy(&m);
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm, std::string _es) 
  : rsm (_rsm)
{
  pthread_mutex_init(&m, NULL);
  es = _es;
  rsm = _rsm;
  rsm->set_state_transfer(this); 
}

lock_server_cache_rsm::~lock_server_cache_rsm()
{
  pthread_mutex_destroy(&m);
}

cachable_lock_rsm&
lock_server_cache_rsm::get_lock(lock_protocol::lockid_t lid) 
{
  ScopedLock ml(&m);
  return lockset[lid];
}

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
				   unsigned int t, 
				   lock_protocol::xid_t xid, int &)
{
  // Can hold mutex across RPCs w/o any chance of remote deadlock as 
  // server only processes one type of request
  cachable_lock_rsm &clck = get_lock(lid);
  ScopedLock ml(&clck.m);
  //  ScopedLock ml(&m);

  int r = rlock_protocol::OK;
  int ret;

  std::string s = "";

  tprintf("[%llu] {%d} Requester: %s, Owner: %s\n", lid, t, id.c_str(), 
	  clck.owner.c_str());

  if (clck.owner.length() == 0) {
    clck.owner = id;
    r = lock_protocol::XOK; // HACK: make this guy the owner
  }
  else {
    tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling [1]\n", lid, t, 
	    id.c_str(), clck.owner.c_str());

    handle h(clck.owner);

    tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling [2]\n", lid, t, 
	    id.c_str(), clck.owner.c_str());

    rpcc *cl = h.safebind();

    tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling [3]\n", lid, t, 
	    id.c_str(), clck.owner.c_str());

    if (cl) {
      pthread_mutex_unlock(&clck.m);
      tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling [4]\n", lid, t, 
	      id.c_str(), clck.owner.c_str());
      r = cl->call(rlock_protocol::transfer, lid, t, id, ret);
      pthread_mutex_lock(&clck.m);
      tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling [5]\n", lid, t, 
	    id.c_str(), clck.owner.c_str());
    }

    // if WRITE, update owner
    if ((lock_protocol::lock_type) t == lock_protocol::WRITE) {
      clck.owner = id;
    }
  }

  tprintf("[%llu] {%d}, Requester: %s, Owner: %s, Returning: %d\n", 
	  lid, t, id.c_str(), 
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
