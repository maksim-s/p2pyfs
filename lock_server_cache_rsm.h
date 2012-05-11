#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>
#include <set>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"

struct xid {
  lock_protocol::xid_t cxid;
  lock_protocol::xid_t sxid;
};

class cachable_lock_rsm {
 public:
  pthread_mutex_t m;
  std::string owner;
  std::set<std::string> copyset;
  std::set<std::string> waiters;
  std::string fowner;
  lock_protocol::lock_type type;
  lock_protocol::lock_type ftype;
  std::map<std::string, struct xid> xids;
  cachable_lock_rsm();
  ~cachable_lock_rsm();
};

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  class rsm *rsm;
  std::map<lock_protocol::lockid_t, cachable_lock_rsm> lockset;
  std::set<lock_protocol::lockid_t> revokeset;
  std::set<lock_protocol::lockid_t> transferset;
  std::set<lock_protocol::lockid_t> retryset;

  pthread_mutex_t m;
  pthread_cond_t revoker_cv;
  pthread_cond_t transferer_cv;
  pthread_cond_t retryer_cv;

  std::string es;

  cachable_lock_rsm& get_lock(lock_protocol::lockid_t);

 public:
  lock_server_cache_rsm(class rsm *rsm = 0, std::string _es = "");
  ~lock_server_cache_rsm();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  void revoker();
  void transferer();
  void retryer();

  std::string marshal_state();
  void unmarshal_state(std::string state);
  int acquire(lock_protocol::lockid_t, std::string id, unsigned int,
	      lock_protocol::xid_t, int &);
  int release(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	      int &);
  int ack(lock_protocol::lockid_t, std::string id, lock_protocol::xid_t,
	  int &);
};

#endif
