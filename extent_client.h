// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "lock_client_cache_rsm.h"
#include "rpc.h"

class extent {
 public:
  extent_protocol::extentid_t id;
  std::string data;
  extent_protocol::attr attr;

  extent();
  extent(extent_protocol::extentid_t);
};

class extent_client : public lock_release_user {
 private:
  std::map<extent_protocol::extentid_t, extent> extentset;
  rpcc *cl;
  pthread_mutex_t m;

 public:
  extent_client();
  ~extent_client();

  extent_protocol::status get(extent_protocol::extentid_t eid, 
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  void flush(extent_protocol::extentid_t);
  void populate(extent_protocol::extentid_t, std::string);
  void fetch(extent_protocol::extentid_t, std::string &);
  void evict(extent_protocol::extentid_t);
  void dorelease(lock_protocol::lockid_t);
  void dopopulate(lock_protocol::lockid_t, std::string);
  void dofetch(lock_protocol::lockid_t, std::string &);
  void doevict(lock_protocol::lockid_t);
};

#endif 

