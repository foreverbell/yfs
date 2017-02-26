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

  if (xid < it->second.client_ctx[id].last_xid) {
    tprintf("stale acquire request of lock %lld from client %s.\n", lid, id.c_str());
    return lock_protocol::STALE;
  } else if (it->second.client_ctx[id].last_xid == xid) {
    const acquire_reply_t &reply = it->second.client_ctx[id].acquire_reply;

    if (reply.status == lock_protocol::OK) {
      r = reply.ret;
    }
    if (!reply.revoke.empty()) {
      task_t task;
      task.lid = lid;
      task.client = reply.revoke;
      revoke_tasks.enq(std::move(task));
    }
    return reply.status;
  }

  it->second.client_ctx[id].last_xid = xid;

  acquire_reply_t &reply = it->second.client_ctx[id].acquire_reply;
  reply.status = lock_protocol::OK;

  while (true) {
    switch (it->second.status) {
      case lock_status::free: {
        it->second.status = lock_status::lent;
        it->second.owner = id;

        reply.ret = r = !it->second.wait_q.empty();

        tprintf("lock %lld is owned by %s now.\n", lid, it->second.owner.c_str());

        return (reply.status = lock_protocol::OK);
      }

      case lock_status::lent:
      case lock_status::revoked: {
        it->second.wait_q.push(id);

        if (it->second.status == lock_status::lent) {
          task_t task;
          task.lid = lid;
          task.client = it->second.owner;
          revoke_tasks.enq(std::move(task));

          it->second.status = lock_status::revoked;

          reply.revoke = it->second.owner;
        }

        return (reply.status = lock_protocol::RETRY);
      }
    }
  }

  return reply.status;
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

  if (xid < it->second.client_ctx[id].last_xid) {
    tprintf("stale release request of lock %lld from client %s.\n", lid, id.c_str());
    return lock_protocol::STALE;
  } else if (xid == it->second.client_ctx[id].last_xid) {
    return it->second.client_ctx[id].release_reply.status;
  }

  it->second.client_ctx[id].last_xid = xid;

  release_reply_t &reply = it->second.client_ctx[id].release_reply;

  if (it->second.status == lock_status::free) {
    tprintf("lock %lld is free.\n", lid);
    return (reply.status = lock_protocol::RPCERR);
  }
  if (it->second.owner != id) {
    tprintf("lock %lld is not owned by client %s.\n", lid, id.c_str());
    return (reply.status = lock_protocol::RPCERR);
  }

  it->second.status = lock_status::free;
  it->second.owner.clear();

  if (!it->second.wait_q.empty()) {
    std::string next = it->second.wait_q.front();
    it->second.wait_q.pop();

    task_t task;
    task.client = std::move(next);
    task.lid = lid;
    retry_tasks.enq(std::move(task));
  }

  return (reply.status = lock_protocol::OK);
}

std::string
lock_server_cache_rsm::marshal_state()
{
  ScopedLock ml(&m);

  marshall m;
  std::map<lock_protocol::lockid_t, lock_t>::const_iterator iter_lock;
  std::map<std::string, client_context_t>::const_iterator iter_ctx;

  m << (int) locks.size();

  for (iter_lock = locks.begin(); iter_lock != locks.end(); ++iter_lock) {
    m << iter_lock->first;

    const lock_t &l = iter_lock->second;

    m << (int) l.status;
    m << l.owner;
    m << l.wait_q;

    m << (int) l.client_ctx.size();
    for (iter_ctx = l.client_ctx.begin(); iter_ctx != l.client_ctx.end(); ++iter_ctx) {
      m << iter_ctx->first << iter_ctx->second;
    }
  }

  return m.str();
}

void
lock_server_cache_rsm::unmarshal_state(std::string state)
{
  ScopedLock ml(&m);

  unmarshall u(state);
  int lock_size, ctx_size;

  locks.clear();

  u >> lock_size;
  for (int i = 0; i < lock_size; ++i) {
    lock_protocol::lockid_t key;
    std::string elem;
    int status;

    u >> key;
    locks[key] = lock_t();

    lock_t &l = locks[key];

    u >> status; l.status = (lock_status) status;
    u >> l.owner;
    u >> l.wait_q;

    u >> ctx_size;
    for (int j = 0; j < ctx_size; ++j) {
      std::string client;
      client_context_t ctx;

      u >> client >> ctx;
      l.client_ctx[client] = std::move(ctx);
    }
  }
}
