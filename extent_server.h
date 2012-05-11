// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "lock_protocol.h"
class extent {
 public:
  extent_protocol::attr attr;
  std::string contents;
  extent();
};

struct transfer_t {
    std::string rid;
    lock_protocol::xid_t rxid;
    lock_protocol::lock_type rtype;
};

class extent_server {
 private:
  int rlock_port;
  std::string hostname;
  std::string id;
  
 protected:
  pthread_mutex_t m;

 public:
  static int last_port;
  extent_server();
  ~extent_server();

  std::map<extent_protocol::extentid_t, struct transfer_t> transferset;
  pthread_cond_t transferer_cv;

  void transferer();
  rlock_protocol::status transfer_handler(extent_protocol::extentid_t, 
					  lock_protocol::xid_t, 
				      	  unsigned int, 
					  std::string, lock_protocol::xid_t,
					  int &);  
};

#endif 







