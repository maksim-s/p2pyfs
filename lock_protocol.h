// lock protocol

#ifndef lock_protocol_h
#define lock_protocol_h

#include "rpc.h"

// Client -> Server
class lock_protocol {
 public:
  enum xxstatus { OK, RETRY, XOK, RPCERR, NOENT, IOERR };
  typedef int status;
  typedef unsigned long long lockid_t;
  typedef unsigned long long xid_t;
  // RPC calls that server receives
  enum rpc_numbers {
    acquire = 0x7001, // i want lock
    release, // UNUSED
    stat // UNUSED
  };
  enum lock_type { READ = 0x0, WRITE, UNUSED };
};

// Server -> Client
class rlock_protocol {
 public:
  enum xxstatus { OK, RPCERR, NOENT, RETRY };
  typedef int status;
  enum rpc_numbers {
    transfer = 0x8001, // send page (+ xfer ownership) to requesting client
    retry, // UNUSED
    revoke // UNUSED
  };
};

// Client -> Client
class clock_protocol {
 public:
  enum xxstatus { OK, RPCERR };
  typedef int status;
  enum rpc_numbers {
    receive = 0x9001, // i am sending you a page (+ copyset)
    invalidate, // invalidate the lock (owner -> clt)
    revoke // i am releasing this lock (clt -> owner)
  };
};

#endif 
