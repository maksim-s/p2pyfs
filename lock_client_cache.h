// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <map>
#include <set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class cached_lock {
 public:
  // State
  enum lock_status { NONE, FREE, LOCKED, ACQUIRING, RELEASING };

  // Mutex
  pthread_mutex_t m;

  // CVs
  pthread_cond_t retry_cv;
  pthread_cond_t free_cv;
  pthread_cond_t release_cv;

  // Owner TID
  pthread_t owner;

  // 'retry' RPC received?
  bool retry;

  // Local waiting threads
  std::set<int> waiters;

  cached_lock();
  ~cached_lock();
  void set_status(lock_status s);
  lock_status status();

 private:
  // Status
  lock_status _status;
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;

  // Mutex
  pthread_mutex_t m;

  // CVs
  pthread_cond_t release_cv;

  // Global Maps
  std::map<lock_protocol::lockid_t, cached_lock> lockset;
  std::map<lock_protocol::lockid_t, int> releaseset;

  // Getters for lockset
  cached_lock& get_lock(lock_protocol::lockid_t);

  // RPC requests to server
  lock_protocol::status sacquire(lock_protocol::lockid_t, cached_lock&);
  lock_protocol::status srelease(lock_protocol::lockid_t, cached_lock&);

 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache();
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
  void releaser();
};

#endif
