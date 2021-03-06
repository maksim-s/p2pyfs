#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client_cache_rsm.h"

class yfs_client {
  extent_client *ec;
  lock_client *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  inum newinum(bool);
  bool addfile(inum, std::string, bool, inum&);

 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  inum lookup(inum, std::string);

  status create(inum, std::string, inum &);
  status readdir(inum, std::vector<dirent> &);
  status setsize(inum, off_t);
  status read(inum, std::string &, off_t, size_t, size_t &);
  status write(inum, const char *, off_t, size_t, size_t &);
  status mkdir(inum, std::string, inum &);
  status unlink(inum, std::string);
};

#endif 
