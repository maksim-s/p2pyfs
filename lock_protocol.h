// lock protocol

#ifndef lock_protocol_h
#define lock_protocol_h

#include "rpc.h"

// Client -> Server
class lock_protocol {
 public:
  enum xxstatus { OK, RETRY, RPCERR, NOENT, IOERR };
  typedef int status;
  typedef unsigned long long lockid_t;
  typedef unsigned long long xid_t;
  // RPC calls that server receives
  enum rpc_numbers {
    acquire = 0x7001,
    release,
    ack,
    stat
  };
  enum lock_type { READ = 0x0, WRITE, UNUSED };
};

// Server -> Client
class rlock_protocol {
 public:
  enum xxstatus { OK, RPCERR };
  typedef int status;
  enum rpc_numbers {
    revoke = 0x8001,
    transfer,
    retry
  };
};

// Client -> Client
class clock_protocol {
 public:
  enum xxstatus { OK, RPCERR };
  typedef int status;
  enum rpc_numbers {
    receive = 0x9001
  };
};

#endif 
