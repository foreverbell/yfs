// the caching lock server implementation

#include "lock_server_cache_rsm.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

static void *
revokethread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache_rsm *sc = (lock_server_cache_rsm *) x;
  sc->retryer();
  return 0;
}

lock_server_cache_rsm::lock_server_cache_rsm(class rsm *_rsm) 
  : rsm (_rsm)
{
  pthread_mutex_init(&m, NULL);

  pthread_t th;

  pthread_create(&th, NULL, &revokethread, (void *) this);
  pthread_create(&th, NULL, &retrythread, (void *) this);

  // Register (un)marshal handler to rsm.
  rsm->set_state_transfer(this);
}

void
lock_server_cache_rsm::revoker()
{
  // We don't need to hold lock here as revoke_tasks is thread-safe.
  task_t task;

  while (true) {
    revoke_tasks.deq(&task);

    if (!rsm->amiprimary()) { // only primary is allowed to contact client.
      continue;
    }

    handle h(task.client);
    rpcc *cl = h.safebind();
    int r;

    if (cl) {
      tprintf("revoking lock %lld owned by client %s.\n", task.lid, task.client.c_str());

      cl->call(rlock_protocol::revoke, task.lid, (lock_protocol::xid_t) 0 /* xid */, r);
    }
  }
}

void
lock_server_cache_rsm::retryer()
{
  task_t task;

  while (true) {
    retry_tasks.deq(&task);

    if (!rsm->amiprimary()) { // only primary is allowed to contact client.
      continue;
    }

    handle h(task.client);
    rpcc *cl = h.safebind();
    int r;

    if (cl) {
      tprintf("retry lock %lld for client %s.\n", task.lid, task.client.c_str());

      cl->call(rlock_protocol::retry, task.lid, (lock_protocol::xid_t) 0 /* xid */, r);
    }
  }
}

// TODO: The server should discard out-of-date requests, but *must* reply
// consistently if the request is a duplication of the latest request from the
// same client.

lock_protocol::status
lock_server_cache_rsm::acquire(lock_protocol::lockid_t lid, std::string id,
                               lock_protocol::xid_t xid, int &r)
{
  ScopedLock ml(&m);

  tprintf("acquire request of lock %lld from client %s.\n", lid, id.c_str());

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);
  if (it == locks.end()) {
    locks[lid] = lock_t();
    it = locks.find(lid);
  }

  // XXX: xid check.
  it->second.last_xid = xid;

  while (true) {
    switch (it->second.status) {
      case lock_status::free: {
        it->second.status = lock_status::lent;
        it->second.nacquire += 1;
        it->second.owner = id;
        r = !it->second.queue.empty();
        tprintf("lock %lld is owned by %s now.\n", lid, it->second.owner.c_str());
        return lock_protocol::OK;
      }

      case lock_status::lent:
      case lock_status::revoked: {
        it->second.queue.push(id);

        if (it->second.status == lock_status::lent) {
          task_t task;
          task.lid = lid;
          task.client = it->second.owner;
          revoke_tasks.enq(std::move(task));

          it->second.status = lock_status::revoked;
        }

        return lock_protocol::RETRY;
      }
    }
  }
}

lock_protocol::status
lock_server_cache_rsm::release(lock_protocol::lockid_t lid, std::string id, 
                               lock_protocol::xid_t xid, int &r)
{
  ScopedLock ml(&m);

  tprintf("release request of lock %lld from client %s.\n", lid, id.c_str());

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);
  if (it == locks.end()) {
    tprintf("lock %lld is not found.\n", lid);
    return lock_protocol::RPCERR;
  }

  // XXX: equality check
  it->second.last_xid = xid;

  if (it->second.status == lock_status::free) {
    tprintf("lock %lld is free.\n", lid);
    return lock_protocol::RPCERR;
  }
  if (it->second.owner != id) {
    tprintf("lock %lld is not owned by client %s.\n", lid, id.c_str());
    return lock_protocol::RPCERR;
  }

  it->second.status = lock_status::free;
  it->second.owner.clear();

  if (!it->second.queue.empty()) {
    std::string next = it->second.queue.front();
    it->second.queue.pop();

    task_t task;
    task.client = std::move(next);
    task.lid = lid;
    retry_tasks.enq(std::move(task));
  }

  return lock_protocol::OK;
}

std::string
lock_server_cache_rsm::marshal_state()
{
  ScopedLock ml(&m);

  marshall m;
  std::map<lock_protocol::lockid_t, lock_t>::iterator iter_lock;

  m << (int) locks.size();

  for (iter_lock = locks.begin(); iter_lock != locks.end(); ++iter_lock) {
    m << iter_lock->first;

    const lock_t &l = iter_lock->second;
    std::queue<std::string> q = l.queue.get();

    m << (int) l.status;
    m << l.nacquire;
    m << l.owner;
    m << (int) q.size();
    while (!q.empty()) {
      m << q.front();
      q.pop();
    }
    m << l.last_xid;
  }

  return m.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  ScopedLock ml(&m);

  unmarshall u(state);
  int map_size;

  locks.clear();

  u >> map_size;
  for (int i = 0; i < map_size; ++i) {
    lock_protocol::lockid_t key;
    std::string elem;
    int size, status;

    u >> key;
    locks[key] = lock_t();

    lock_t &l = locks[key];

    u >> status; l.status = (lock_status) status;
    u >> l.nacquire;
    u >> l.owner;
    u >> size;
    while (size > 0) {
      size -= 1;
      u >> elem;
      l.queue.push(elem);
    }
    u >> l.last_xid;
  }
}

lock_protocol::status
lock_server_cache_rsm::stat(lock_protocol::lockid_t lid, int &r)
{
  ScopedLock ml(&m);

  tprintf("stat request of lock %lld.\n", lid);

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);
  if (it == locks.end()) {
    r = 0;
  } else {
    r = it->second.nacquire;
  }

  return lock_protocol::OK;
}
