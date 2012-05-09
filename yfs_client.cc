// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
//#include "lock_client.h"
//#include "lock_client_cache.h"
#include "lock_client_cache_rsm.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache_rsm(lock_dst, ec);
  srand(getpid());
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

yfs_client::inum
yfs_client::newinum(bool is_file)
{
  bool success = false;
  inum i = 0;

  while (true) {
    if (is_file) i = (rand() | 0x80000000) & 0xffffffff;
    else i = rand() & 0x7fffffff;
    lc->acquire(i);
    // if (i && ec->get(i, nouse) != extent_protocol::OK) {
    if (i) {
      ec->put(i, "");
      success = true;
    }
    lc->release(i);
    if (success) break;
  }

  return i;
}

bool
yfs_client::addfile(inum parent, std::string name, bool is_file, inum &myinum)
{
  assert(!isfile(parent)); // parent must be directory inode
  assert(name.length()); // name length must be > 0

  bool success = false;
  std::string contents;
  
  lc->acquire(parent);
  if (ec->get(parent, contents) == extent_protocol::OK) {
    std::istringstream is(contents);
    std::ostringstream os;
    std::string line;
    size_t sep;
    int cmp;
    inum i = 0;

    while(getline(is, line)) {
      if (!line.length()) continue; // eof
      sep = line.find('/');

      // Check that line is valid entry!
      assert(sep != 0 && sep != line.length() - 1 && sep != std::string::npos);

      if (!i) { // If file has not been added yet
	cmp = strcmp(line.substr(0, sep).c_str(), name.c_str());
	if (cmp == 0) { // Name already exists in dir
	  sscanf(line.substr(sep + 1).c_str(), "%llu", &i);
	  break;
	}
	else if (cmp > 0) { // Lexicographically canonical place
	  i = newinum(is_file);
	  os << name << "/" << i << std::endl;
	  success = true;
	}
      }
      os << line << std::endl;
    }

    if (!i) { // Name should go at the end?
      i = newinum(is_file);
      os << name << "/" << i << std::endl;
      success = true;
    }

    if (success) { // Update parent dir
      ec->put(parent, os.str());
    }

    myinum = i;
  }

  lc->release(parent);
  return success;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return !isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;

  lc->acquire(inum);
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;

  lc->acquire(inum);
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(inum);
  return r;
}

yfs_client::inum 
yfs_client::lookup(inum parent, std::string name) 
{
  printf("lookup %016llx -> %s\n", parent, name.c_str());
  std::string contents;
  inum i = 0; // All inode # > 0, so 0 represets file doesn't exist

  lc->acquire(parent);
  if (ec->get(parent, contents) == extent_protocol::OK) {
    std::istringstream is(contents);
    std::string line;
    while (getline(is, line)) {
      if (line.find(name) == 0 && line[name.length()] == '/') {
	sscanf(line.substr(name.length() + 1).data(), "%llu", &i);
	break;
      }
    }
  }

  lc->release(parent);
  return i;
}

yfs_client::status
yfs_client::create(inum parent, std::string name, inum &myinum)
{
  printf("create %016llx -> %s\n", parent, name.c_str());
  status r;

  if (addfile(parent, name, true, myinum)) {
    r = OK;
  }
  else {
    r = EXIST;
  }

  return r;
}

yfs_client::status
yfs_client::readdir(inum parent, std::vector<dirent> &children)
{
  printf("readdir %016llx\n", parent);
  std::string contents;
  status r = NOENT;

  lc->acquire(parent);
  if (ec->get(parent, contents) == extent_protocol::OK) {
    std::istringstream is(contents);
    std::string line;
    size_t sep;
    while (getline(is, line)) {
      if (!line.length()) continue; // eof
      sep = line.find('/');

      // Check that line is valid entry!
      assert(sep != 0 && sep != line.length() - 1 && sep != std::string::npos);

      dirent entry;
      entry.name = line.substr(0, sep);
      sscanf(line.substr(sep + 1).c_str(), "%llu", &entry.inum);
      children.push_back(entry);
    }
    r = OK;
  }

  lc->release(parent);
  return r;
}

yfs_client::status
yfs_client::setsize(inum file, off_t size) 
{
  printf("setsize %016llx -> %d\n", file, (unsigned int) size);
  std::string contents;
  status r = NOENT;

  lc->acquire(file);
  if (ec->get(file, contents) == extent_protocol::OK) {
    int len = contents.length();

    if (len < size) { // extend
      contents.resize(size, '\0');
      ec->put(file, contents);
    }
    else if (len > size) { // truncate
      ec->put(file, contents.substr(0, size));
    }
    r = OK;
  }

  lc->release(file);
  return r;
}

yfs_client::status 
yfs_client::read(inum file, std::string &buf, off_t offset, size_t nbytes, size_t & nread)
{
  printf("read %016llx -> %d, %d\n", file, (unsigned int) offset, (unsigned int) nbytes);
  std::string contents;
  status r = NOENT;

  lc->acquire(file);
  if (ec->get(file, contents) == extent_protocol::OK) {
    int len = contents.length();
    if (offset >= len) {
      nread = 0;
      buf = std::string();
    }
    else {
      nread = nbytes > len - offset ? len - offset : nbytes;
      buf = contents.substr(offset, nread);
    }
    r = OK;
  }

  lc->release(file);
  return r;
}

yfs_client::status 
yfs_client::write(inum file, const char *buf, off_t offset, size_t nbytes, size_t &nwrite)
{
  printf("write %016llx -> %d, %d\n", file, (unsigned int) offset, (unsigned int) nbytes);
  std::string contents;
  status r = NOENT;

  lc->acquire(file);
  if (ec->get(file, contents) == extent_protocol::OK) {
    int len = contents.length();
    int end = offset + nbytes;
    if (end > len) {
      contents.resize(end);
    }
    contents.replace(offset, nbytes, std::string(buf, nbytes));
    ec->put(file, contents);
    nwrite = nbytes;
    r = OK;
  }

  lc->release(file);
  return r;
}

yfs_client::status
yfs_client::mkdir(inum parent, std::string name, inum &myinum)
{
  printf("mkdir %016llx -> %s\n", parent, name.c_str());
  status r;
  if (addfile(parent, name, false, myinum)) {
    r = OK;
  }
  else {
    r = EXIST;
  }

  return r;
}

yfs_client::status
yfs_client::unlink(inum parent, std::string name)
{
  printf("unlink %016llx -> %s\n", parent, name.c_str());
  assert(!isfile(parent)); // parent must be directory inode
  assert(name.length()); // name length must be > 0

  status r = NOENT;
  std::string contents;
  
  lc->acquire(parent);
  if (ec->get(parent, contents) == extent_protocol::OK) {
    std::istringstream is(contents);
    std::ostringstream os;
    std::string line;
    size_t sep;
    int cmp;
    inum i = 0;

    while(getline(is, line)) {
      if (!line.length()) continue; // eof
      sep = line.find('/');

      // Check that line is valid entry!
      assert(sep != 0 && sep != line.length() - 1 && sep != std::string::npos);

      if (!i) {
	cmp = strcmp(line.substr(0, sep).c_str(), name.c_str());
	if (cmp == 0) { // Name exists in dir
	  sscanf(line.substr(sep + 1).c_str(), "%llu", &i);
	  continue;
	}
	else if (cmp > 0) { // Lexicographically canonical place passed
	  break;
	}
      }
      os << line << std::endl;
    }

    if (!i || !isfile(i)) { // Failed to find name?
      r = NOENT;
      goto release;
    }

    ec->put(parent, os.str());

    lc->acquire(i);
    r = ec->remove(i) == extent_protocol:: OK ? OK : NOENT;
    lc->release(i);
  }

 release:
  lc->release(parent);
  return r;
}
