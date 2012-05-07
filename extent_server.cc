// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent::extent() {
  time_t t = time(NULL);
  attr.atime = t;
  attr.mtime = t;
  attr.ctime = t;
  attr.size = 0;
}

extent_server::extent_server() {
  int useless;
  pthread_mutex_init(&m_extentset, NULL);
  // Create empty root dir entry on startup 
  put(0x1, "", useless); 
}

extent_server::~extent_server()
{
  pthread_mutex_destroy(&m_extentset);
}


bool extent_server::isfile(extent_protocol::extentid_t id) {
  if (id & 0x80000000) {
    return true;
  }
  return false;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("extent_server::put %llu->%s\n", id, buf.c_str()); 
  // You fill this in for Lab 2.
  int r = extent_protocol::OK;
  extent *extent = NULL;
  pthread_mutex_lock(&m_extentset);
  extent = &extentset[id];
  pthread_mutex_unlock(&m_extentset);

  time_t t = time(NULL);
  extent->attr.atime = t;
  extent->attr.mtime = t;
  extent->attr.ctime = t;

  if (isfile(id)) {
    extent->attr.size = buf.size();
  }
  else {
    extent->attr.size = 4096; // Directory size
  }

  extent->contents = buf;

  return r;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server::get %llu\n", id);
  // You fill this in for Lab 2.
  int r = extent_protocol::NOENT;
  extent *extent = NULL;
  pthread_mutex_lock(&m_extentset);
  if (extentset.count(id)) extent = &extentset[id];
  pthread_mutex_unlock(&m_extentset);

  if (extent) {
    r = extent_protocol::OK;
    time_t t = time(NULL);
    extent->attr.atime = t;
    buf = extent->contents;
  }

  return r;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server::getattr %llu\n", id);
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  int r = extent_protocol::NOENT;
  extent *extent = NULL;
  pthread_mutex_lock(&m_extentset);
  if (extentset.count(id)) extent = &extentset[id];
  pthread_mutex_unlock(&m_extentset);

  if (extent) {
    r = extent_protocol::OK;
    a.size = extent->attr.size;
    a.atime = extent->attr.atime;
    a.ctime = extent->attr.ctime;
    a.mtime = extent->attr.mtime;
  }

  return r;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server::remove %llu\n", id);
  // You fill this in for Lab 2.
  pthread_mutex_lock(&m_extentset);
  int removed = extentset.erase(id);
  pthread_mutex_unlock(&m_extentset);
  return removed ? extent_protocol::OK : extent_protocol::NOENT;
}
