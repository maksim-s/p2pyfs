#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>
#include <set>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

class cachable_lock_rsm {
 public:
  pthread_mutex_t m;
  std::string owner;
  cachable_lock_rsm();
  ~cachable_lock_rsm();
};

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  class rsm *rsm;
  std::map<lock_protocol::lockid_t, cachable_lock_rsm> lockset;
  std::string es;
  pthread_mutex_t m;

 public:
  lock_server_cache_rsm(class rsm *rsm = 0, std::string _es = "");
  ~lock_server_cache_rsm();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);

  cachable_lock_rsm&
  get_lock(lock_protocol::lockid_t lid);

  std::string marshal_state();
  void unmarshal_state(std::string state);

  int acquire(lock_protocol::lockid_t, std::string id, unsigned int,
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
