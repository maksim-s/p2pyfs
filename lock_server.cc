// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock::lock() 
{
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&cv, NULL);
  locked = false;
  nacquire = 0;
}

lock::~lock()
{
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&cv);
}

int lock::stat()
{
  return nacquire;
}

void lock::acquire()
{
  ScopedLock ml(&m);
  while (locked) {
    pthread_cond_wait(&cv, &m);
  }
  locked = true;
  nacquire++;
}

void lock::release()
{
  ScopedLock ml(&m);
  if (locked) { // TODO: What if not locked?
    locked = false;
    // signal should theoretically work because we maintain a lock/lid
    // but I use broadcast just to avoid any potential bugs
    pthread_cond_broadcast(&cv);
  }
}

lock_server::lock_server() 
{
  pthread_mutex_init(&m_lockset, NULL);
}

lock_server::~lock_server()
{
  pthread_mutex_destroy(&m_lockset);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  lock *lock = NULL;
  printf("stat %llu [clt %d]\n", lid, clt);
  pthread_mutex_lock(&m_lockset);
  if (lockset.count(lid)) lock = &lockset[lid];
  pthread_mutex_unlock(&m_lockset);
  if (lock) r = lock->stat();
  else r = 0;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  lock *lock = NULL;
  printf("acquire %llu request [clt %d]\n", lid, clt);
  pthread_mutex_lock(&m_lockset);
  lock = &lockset[lid]; // Adds new lock entry, if absent. 
  pthread_mutex_unlock(&m_lockset);
  lock->acquire(); // Call blocks!
  printf("acquire %llu granted [clt %d]\n", lid, clt);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid,  int &r)
{
  lock_protocol::status ret;
  lock *lock = NULL;
  printf("release %llu request [clt %d]\n", lid, clt);
  pthread_mutex_lock(&m_lockset);
  if (lockset.count(lid)) lock = &lockset[lid];
  pthread_mutex_unlock(&m_lockset);
  if (lock) {
    lock->release();
    ret = lock_protocol::OK;
    printf("release %llu completed [clt %d]\n", lid, clt);
  }
  else { // No such lock exists yet
    ret = lock_protocol::NOENT;
    printf("release %llu failed [clt %d]\n", lid, clt);
  }
  return ret;
}
