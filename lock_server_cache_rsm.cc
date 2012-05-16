// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

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

int lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
				   unsigned int t, 
				   lock_protocol::xid_t xid, int &)
{
  // Can hold mutex across RPCs w/o any chance of remote deadlock as 
  // server only processes one type of request
  ScopedLock ml(&m);
  int r = rlock_protocol::OK;
  int ret;

  std::string s = "";

  tprintf("[%llu] {%d} Requester: %s, Owner: %s\n", lid, t, id.c_str(), 
	  owners[lid].c_str());

  if (owners[lid].length() == 0) {
    owners[lid] = id;
    r = lock_protocol::XOK; // HACK: make this guy the owner
  }
  else {
    tprintf("[%llu] {%d} Requester: %s, Owner: %s Handling...\n", lid, t, 
	    id.c_str(), owners[lid].c_str());
    handle h(owners[lid]);
    rpcc *cl = h.safebind();
    if (cl) {
      r = cl->call(rlock_protocol::transfer, lid, t, id, ret);
    }

    // if WRITE, update owner
    if ((lock_protocol::lock_type) t == lock_protocol::WRITE) {
      owners[lid] = id;
    }
  }

  tprintf("[%llu] {%d}, Requester: %s, Owner: %s, Returning: %d\n", 
	  lid, t, id.c_str(), 
	  owners[lid].c_str(), r);

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
