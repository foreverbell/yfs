// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server() {
  pthread_mutex_init(&m, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("stat request from clt %d\n", clt);

  ScopedLock ml(&m);
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end()) {
    return lock_protocol::IOERR;
  }

  r = it->second.nacquire;

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("acquire request from clt %d\n", clt);

  ScopedLock ml(&m);

  // Inserting an new element from std::map does not invalid other iterators,
  // it is safe to find it before we wait the conditional variable.
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end()) {
    locks[lid] = lock_t();
    it = locks.find(lid);
  }

  while (it->second.status == lock_status::locked) {
    pthread_cond_wait(&it->second.free_c, &m);
  }

  // Now the lock is free.
  it->second.status = lock_status::locked;
  it->second.nacquire += 1;

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("release request from clt %d\n", clt);

  ScopedLock ml(&m);
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end() || it->second.status == lock_status::free) {
    return lock_protocol::IOERR;
  }

  it->second.status = lock_status::free;
  pthread_cond_signal(&it->second.free_c);

  return lock_protocol::OK;
}
