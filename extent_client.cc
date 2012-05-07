// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent::extent()
{
  id = 0;
  dirty = false;
  removed = false;
}

extent::extent(extent_protocol::extentid_t eid)
{
  id = eid;
  dirty = false;
  removed = false;
}

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  printf("get %llu\n", eid);
  extent_protocol::status ret = extent_protocol::NOENT;
  pthread_mutex_lock(&m);
  if (extentset.count(eid)) {
    extent &extent = extentset[eid];
    if (extent.removed) goto release;
    ret = extent_protocol::OK;
    buf = extent.data;
  }
  else {
    ret = cl->call(extent_protocol::get, eid, buf);
    if (ret != extent_protocol::OK) goto release;
    extent &extent = extentset[eid];
    ret = cl->call(extent_protocol::getattr, eid, extent.attr);
    assert(ret == extent_protocol::OK);
    extent.data = std::string(buf);
  }
 release:
  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  printf("getattr %llu\n", eid);
  extent_protocol::status ret = extent_protocol::NOENT;
  pthread_mutex_lock(&m);
  if (extentset.count(eid)) {
    extent &extent = extentset[eid];
    if (extent.removed) goto release;
    ret = extent_protocol::OK;
    attr = extent.attr;
  }
  else {
    extent &extent = extentset[eid];
    ret = cl->call(extent_protocol::getattr, eid, extent.attr);
    if (ret != extent_protocol::OK) goto release;
    ret = cl->call(extent_protocol::get, eid, extent.data);
    assert(ret == extent_protocol::OK);
    attr = extent.attr;
  }
 release:
  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  printf("put %llu\n", eid);
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  if (!extentset.count(eid)) {
    extentset[eid] = extent(eid);
  }
  extent &extent = extentset[eid];
  extent.data = std::string(buf);
  time((time_t *) &extent.attr.atime);
  time((time_t *) &extent.attr.ctime);
  time((time_t *) &extent.attr.mtime);
  extent.dirty = true;
  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  if (!extentset.count(eid)) {
    extentset[eid] = extent(eid);
  }
  extent &extent = extentset[eid];
  extent.removed = true;
  pthread_mutex_unlock(&m);
  return ret;
}

void 
extent_client::flush(extent_protocol::extentid_t eid)
{
  int r, ret;
  pthread_mutex_lock(&m);
  if (extentset.count(eid)) {
    extent &extent = extentset[eid];
    if (extent.removed) {
      ret = cl->call(extent_protocol::remove, eid, r);
    }
    else if (extent.dirty) {
      ret = cl->call(extent_protocol::put, eid, extent.data, r);
    }
  }
  extentset.erase(eid);
  pthread_mutex_unlock(&m);
}


void 
extent_client::dorelease(lock_protocol::lockid_t lid)
{
  this->flush(lid);
}
