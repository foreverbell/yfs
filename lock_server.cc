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
    return lock_protocol::RPCERR;
  }

  r = it->second.nacquire;

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("acquire request from clt %d\n", clt);

  ScopedLock ml(&m);

  while (true) {
    std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

    if (it == locks.end()) {
      locks[lid] = lock_t();
      it = locks.find(lid);
    }

    if (it->second.status == lock_status::free) {
      it->second.status = lock_status::locked;
      it->second.nacquire += 1;

      return lock_protocol::OK;
    } else { // acquired
      pthread_cond_wait(&it->second.free_c, &m);
    }
  }

  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("release request from clt %d\n", clt);

  ScopedLock ml(&m);
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end() || it->second.status == lock_status::free) {
    return lock_protocol::RPCERR;
  }

  it->second.status = lock_status::free;
  pthread_cond_signal(&it->second.free_c);

  return lock_protocol::OK;
}
