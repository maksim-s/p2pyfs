// lock client interface.

#ifndef lock_client_cache_rsm_h

#define lock_client_cache_rsm_h

#include <string>
#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

#include "rsm_client.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual void dopopulate(lock_protocol::lockid_t, std::string data) = 0;
  virtual void dofetch(lock_protocol::lockid_t, std::string &data) = 0;
  virtual void doevict(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class cached_lock_rsm {
 public:
  // State
  enum lock_status { NONE, FREE, LOCKED, ACQUIRING, REVOKING };

  // Mutex
  pthread_mutex_t m;

  // CVs
  pthread_cond_t retry_cv;
  pthread_cond_t receive_cv;
  pthread_cond_t readfree_cv;
  pthread_cond_t allfree_cv;
  pthread_cond_t none_cv;

  // 'acquire' flow completed?
  bool acquired;

  // 'retry' RPC received?
  bool retried;

  // 'receive' RPC received?
  bool received;

  // xid for last successful acquire
  lock_protocol::xid_t sxid;

  // xid for last acquire sent to server
  lock_protocol::xid_t cxid;

  // Owners 
  std::set<pthread_t> owners;

  // Owner type R/W
  lock_protocol::lock_type otype;

  // Server leased type R/W
  lock_protocol::lock_type stype;

  cached_lock_rsm();
  ~cached_lock_rsm();
  void set_status(lock_status s);
  lock_status status();

 private:
  // Status
  lock_status _status;
};

struct transfer_t {
  std::string rid;
  lock_protocol::xid_t rxid;
  lock_protocol::lock_type rtype;
};

class lock_client_cache_rsm;

// Clients that caches locks.  The server can revoke locks using 
// lock_revoke_server.
class lock_client_cache_rsm : public lock_client {
 private:
  rsm_client *rsmc;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  lock_protocol::xid_t xid;

  // Mutex
  pthread_mutex_t m;

  // CVs
  pthread_cond_t revoker_cv;
  pthread_cond_t transferer_cv;
  pthread_cond_t acker_cv;

  // Global Maps
  std::map<lock_protocol::lockid_t, cached_lock_rsm> lockset;
  std::map<lock_protocol::lockid_t, int> revokeset;
  std::map<lock_protocol::lockid_t, struct transfer_t> transferset;
  std::map<lock_protocol::lockid_t, int> ackset;

  // Getter for lockset
  cached_lock_rsm& get_lock(lock_protocol::lockid_t);

  // RPC requests to server
  lock_protocol::status sacquire(lock_protocol::lockid_t, 
				 lock_protocol::lock_type, cached_lock_rsm&);
  lock_protocol::status srelease(lock_protocol::lockid_t, cached_lock_rsm&);
  lock_protocol::status sack(lock_protocol::lockid_t, cached_lock_rsm&);

 public:
  static int last_port;
  lock_client_cache_rsm(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache_rsm();

  void revoker();
  void transferer();
  void acker();

  lock_protocol::status acquire(lock_protocol::lockid_t, 
				lock_protocol::lock_type);
  virtual lock_protocol::status release(lock_protocol::lockid_t);

  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
					lock_protocol::xid_t, int &);
  rlock_protocol::status transfer_handler(lock_protocol::lockid_t, 
					  lock_protocol::xid_t, 
				      	  unsigned int, 
					  std::string, lock_protocol::xid_t,
					  int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
				       lock_protocol::xid_t, int &);

  clock_protocol::status receive_handler(lock_protocol::lockid_t, 
					 lock_protocol::xid_t, 
					 std::string, int &);
};


#endif
