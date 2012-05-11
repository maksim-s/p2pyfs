// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "handle.h"
#include "marshall.h"

extent::extent() {
  time_t t = time(NULL);
  attr.atime = t;
  attr.mtime = t;
  attr.ctime = t;
  attr.size = 0;
  contents = "";
}

static void *
transferthread(void *x)
{
  extent_server *cc = (extent_server *) x;
  cc->transferer();
  return 0;
}


int extent_server::last_port = 0;
extent_server::extent_server() {
  pthread_cond_init(&transferer_cv, NULL);
  pthread_mutex_init(&m, NULL);
  pthread_t th;
  int r = pthread_create(&th, NULL, &transferthread, (void *) this);
  VERIFY (r == 0);  
}


extent_server::~extent_server()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&transferer_cv);
}

void extent_server::transferer()
{
 while (true) {
    pthread_mutex_lock(&m);
    while (transferset.empty()) {
      pthread_cond_wait(&transferer_cv, &m);
    }
    extent_protocol::extentid_t eid;
    std::map<extent_protocol::extentid_t, struct transfer_t>::iterator it;
    it = transferset.begin();
    struct transfer_t t = it->second;
    eid = it->first;
    transferset.erase(it); // Remove from set
    pthread_mutex_unlock(&m);
    printf("[%s] transferer -> %llu, %s\n", id.c_str(), eid, t.rid.c_str());

    handle h(t.rid);
    rpcc *cl = h.safebind();
    if (cl) {
      int ret;
      extent e = extent();
      std::string data;
      marshall rep;
      rep << true;
      rep << e.contents;
      rep << e.attr.atime;
      rep << e.attr.mtime;
      rep << e.attr.ctime;
      rep << e.attr.size;
      data = rep.str();
      int r = cl->call(clock_protocol::receive, eid, t.rxid, data, ret);
      assert(r == clock_protocol::OK);
      printf("[%s] transferer done -> %llu, %s\n", id.c_str(), eid, 
	     t.rid.c_str());
    }
    else {
      printf("[%s] transferer -> %llu, %s failure!\n", id.c_str(), eid, 
	     t.rid.c_str());
    }
  }
}

rlock_protocol::status
extent_server::transfer_handler(extent_protocol::extentid_t eid, 
				lock_protocol::xid_t xid, 
				unsigned int rtype,
				std::string rid, 
				lock_protocol::xid_t rxid,
				int &)
{
  int r = rlock_protocol::OK;
  printf("[%s] transfer_handler -> %llu, %d, %s\n", id.c_str(), eid, rtype, 
	 rid.c_str());
  pthread_mutex_lock(&m);
  struct transfer_t t;
  t.rid = rid;
  t.rxid = rxid;
  t.rtype = (lock_protocol::lock_type) rtype;
  transferset[eid] = t;
  pthread_cond_signal(&transferer_cv); // At most one thread waiting
  pthread_mutex_unlock(&m);
  return r;
}
