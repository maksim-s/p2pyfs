// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent {
 public:
  extent_protocol::attr attr;
  std::string contents;
  extent();
};

class extent_server {
 private:
  bool isfile(extent_protocol::extentid_t id);

 protected:
  std::map<extent_protocol::extentid_t, extent> extentset;
  pthread_mutex_t m_extentset;

 public:
  extent_server();
  ~extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







