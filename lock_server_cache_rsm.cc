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
}

void
lock_server_cache_rsm::revoker()
{
  task_t task;

  while (true) {
    revoke_tasks.deq(&task);

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
  std::ostringstream ost;
  std::string r;

  // TODO
  return r;
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  // TODO
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
