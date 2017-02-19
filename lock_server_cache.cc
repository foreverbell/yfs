// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&m, NULL);
}

lock_protocol::status
lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &)
{
  ScopedLock ml(&m);

  tprintf("acquire request of lock %lld, client %s.\n", lid, id.c_str());

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);
  if (it == locks.end()) {
    locks[lid] = lock_t();
    it = locks.find(lid);
  }

  while (true) {
    switch (it->second.status) {
      case lock_status::free: {
        it->second.status = lock_status::lent;
        it->second.nacquire += 1;
        it->second.owner = id;
        tprintf("lock %lld is owned by %s.\n", lid, it->second.owner.c_str());
        return lock_protocol::OK;
      }

      case lock_status::lent: {
        it->second.queue.push(id);

        handle h(it->second.owner);
        rpcc *cl = h.safebind();
        int r;

        if (cl) {
          tprintf("revoking lock %lld owned by client %s.\n", lid, it->second.owner.c_str());
          it->second.status = lock_status::revoked;
          pthread_mutex_unlock(&m);
          cl->call(rlock_protocol::revoke, lid, r);
          pthread_mutex_lock(&m);
        }

        return lock_protocol::RETRY;
      }

      case lock_status::revoked: {
        it->second.queue.push(id);

        return lock_protocol::RETRY;
      }
    }
  }
}

lock_protocol::status
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &)
{
  ScopedLock ml(&m);

  tprintf("release request of lock %lld, client %s.\n", lid, id.c_str());

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);
  if (it == locks.end() || it->second.status == lock_status::free) {
    tprintf("lock %lld is not found or free lock.\n", lid);
    return lock_protocol::RPCERR;
  }
  if (it->second.owner != id) {
    tprintf("lock %lld is not owned by client %s.\n", lid, id.c_str());
    return lock_protocol::RPCERR;
  }

  it->second.status = lock_status::free;
  it->second.owner.clear();

  // Let two clients retry, so we can revoke client 1 when client 2 is acquiring
  // the lock.
  std::vector<std::string> top2;

  for (int i = 0; i < 2 && !it->second.queue.empty(); ++i) {
    top2.push_back(it->second.queue.front());
    it->second.queue.pop();
  }

  for (const std::string &next : top2) {
    handle h(next);
    rpcc *cl = h.safebind();
    int r;

    if (cl) {
      tprintf("retry lock %lld for client %s.\n", lid, next.c_str());
      pthread_mutex_unlock(&m);
      cl->call(rlock_protocol::retry, lid, r);
      pthread_mutex_lock(&m);
    }
  }

  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request of lock %lld.\n", lid);
  r = 0;
  return lock_protocol::OK;
}
