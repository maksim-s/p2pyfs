// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock {
 private:
  pthread_mutex_t m;
  pthread_cond_t cv;
  bool locked;
  int nacquire;

 public:
  lock();
  ~lock();
  int stat();
  void acquire();
  void release();
};

class lock_server {

 protected:
  std::map<lock_protocol::lockid_t, lock> lockset;
  pthread_mutex_t m_lockset;

 public:
  lock_server();
  ~lock_server();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &r);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &r);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &r);
};

#endif 







