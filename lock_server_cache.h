#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <set>
#include <list>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "handle.h"

class cachable_lock {
 public:
  pthread_mutex_t m;
  std::string owner;
  std::string expected;
  std::list<std::string> waiters;
  bool revoking;
  int nacquire;
  cachable_lock();
  ~cachable_lock();
};

class lock_server_cache {
 private:
  std::map<lock_protocol::lockid_t, cachable_lock> lockset;
  std::set<lock_protocol::lockid_t> revokeset;
  std::set<lock_protocol::lockid_t> releasedset;

  pthread_mutex_t m;
  pthread_cond_t revoke_cv;
  pthread_cond_t retry_cv;

  cachable_lock& get_lock(lock_protocol::lockid_t);
  void revoke(lock_protocol::lockid_t, cachable_lock &);

 public:
  lock_server_cache();
  ~lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
  void retryer();
  void revoker();
};

#endif
