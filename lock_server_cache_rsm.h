#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>
#include <set>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  class rsm *rsm;
  std::map<lock_protocol::lockid_t, std::string> owners;
  std::string es;
  pthread_mutex_t m;

 public:
  lock_server_cache_rsm(class rsm *rsm = 0, std::string _es = "");
  ~lock_server_cache_rsm();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);

  std::string marshal_state();
  void unmarshal_state(std::string state);

  int acquire(lock_protocol::lockid_t, std::string id, unsigned int,
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
};

#endif
