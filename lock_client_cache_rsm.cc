// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache_rsm.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#include "rsm_client.h"

int lock_client_cache_rsm::last_port = 0;

lock_client_cache_rsm::lock_client_cache_rsm(std::string xdst, class lock_release_user *_lu)
  : lu(_lu)
{
  srand(time(NULL) ^ last_port);
  int rlock_port = ((rand() % 32000) | (0x1 << 10));

  id = "127.0.0.1:" + std::to_string(rlock_port);
  last_port = rlock_port;

  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache_rsm::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache_rsm::retry_handler);

  xid = 0;

  pthread_mutex_init(&m, NULL);

  rsmc = new rsm_client(xdst);
}

// We don't need releaser thread here. Check the step one guidance for details.
// https://pdos.csail.mit.edu/archive/6.824-2012/labs/lab-7.html.

lock_protocol::status
lock_client_cache_rsm::acquire_impl(
    lock_protocol::lockid_t lid,
    std::map<lock_protocol::lockid_t, lock_t>::iterator it)
{
  VERIFY(it->second.status == lock_status::none);

  it->second.status = lock_status::acquiring;

  lock_protocol::status ret;
  int r = 0;

  while (true) {
    // Assign a new sequence number for this acquire.
    xid += 1;

    pthread_mutex_unlock(&m);
    ret = rsmc->call(lock_protocol::acquire, lid, id, xid, r);
    pthread_mutex_lock(&m);

    if (ret == lock_protocol::OK || ret != lock_protocol::RETRY) {
      break;
    }

    struct timeval now;
    struct timespec next_timeout;

    gettimeofday(&now, NULL);
    next_timeout.tv_sec = now.tv_sec + 3;
    next_timeout.tv_nsec = 0;

    while (!it->second.should_retry) {
      // Retry automatically every 3 seconds.
      // TODO: what if spurious wakeup?
      if (pthread_cond_timedwait(&it->second.retry_c, &m, &next_timeout) == ETIMEDOUT) {
        break;
      }
    }
    it->second.should_retry = false;
  }

  if (ret == lock_protocol::OK) {
    it->second.status = lock_status::free;
    if (r) { // other clients are also waiting for the lock.
      it->second.revoked = true;
    }
  } else {
    it->second.status = lock_status::none; // fail to acquire, keep it as none.
  }

  return ret;
}

lock_protocol::status
lock_client_cache_rsm::release_impl(
    lock_protocol::lockid_t lid,
    std::map<lock_protocol::lockid_t, lock_t>::iterator it)
{
  lock_status status = it->second.status;

  VERIFY(status == lock_status::locked || status == lock_status::free);

  it->second.status = lock_status::releasing;

  if (lu != NULL) {
    lu->dorelease(lid);
  }

  // Assign a new sequence number for this release.
  xid += 1;

  lock_protocol::status ret;
  int r;

  pthread_mutex_unlock(&m);
  ret = rsmc->call(lock_protocol::release, lid, id, xid, r);
  pthread_mutex_lock(&m);

  if (ret == lock_protocol::OK) {
    it->second.owner = 0;
    it->second.revoked = false;
    it->second.status = lock_status::none;
  } else {
    it->second.status = status; // fail to release, keep it as old status.
  }

  return ret;
}

lock_protocol::status
lock_client_cache_rsm::acquire(lock_protocol::lockid_t lid)
{
  ScopedLock ml(&m);

  lock_protocol::status ret;
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end()) {
    locks[lid] = lock_t();
    it = locks.find(lid);
  }

  while (true) {
    switch (it->second.status) {
      case lock_status::none: {
        ret = acquire_impl(lid, it);
        if (ret != lock_protocol::OK) {
          return ret;
        }
        it->second.status = lock_status::locked;
        it->second.owner = pthread_self();
        return lock_protocol::OK;
      }

      case lock_status::free: {
        it->second.status = lock_status::locked;
        it->second.owner = pthread_self();
        return lock_protocol::OK;
      }

      case lock_status::locked:
      case lock_status::acquiring:
      case lock_status::releasing: {
        while (it->second.status != lock_status::free && it->second.status != lock_status::none) {
          pthread_cond_wait(&it->second.free_c, &m);
        }
        if (it->second.status == lock_status::none) {
          continue;  // acquire the lock in next while loop.
        }
        it->second.status = lock_status::locked;
        it->second.owner = pthread_self();
        return lock_protocol::OK;
      }
    }
  }

  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid, bool flush)
{
  ScopedLock ml(&m);

  lock_protocol::status ret;
  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end() || it->second.status != lock_status::locked) {
    tprintf("lock %lld is not found or bad status.\n", lid);
    return lock_protocol::RPCERR;
  }
  if (it->second.owner != pthread_self()) {
    tprintf("lock %lld is not owned by thread %ld.\n", lid, pthread_self());
    return lock_protocol::RPCERR;
  }

  if (it->second.revoked || flush) {
    ret = release_impl(lid, it);
    if (ret != lock_protocol::OK) {
      return ret;
    }
  } else {
    it->second.status = lock_status::free;
    it->second.owner = 0;
  }

  pthread_cond_signal(&it->second.free_c);

  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache_rsm::release(lock_protocol::lockid_t lid)
{
  return release(lid, false /* flush */);
}

// XXX: Do we really need xid here?
rlock_protocol::status
lock_client_cache_rsm::revoke_handler(lock_protocol::lockid_t lid, lock_protocol::xid_t, int &)
{
  ScopedLock ml(&m);

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end()) {
    tprintf("lock %lld not found.\n", lid);
    return lock_protocol::RPCERR;
  }

  if (it->second.status == lock_status::free) {
    if (release_impl(lid, it) != lock_protocol::OK) {
      return rlock_protocol::RPCERR;
    }

    pthread_cond_signal(&it->second.free_c);
  } else {
    it->second.revoked = true;
  }
  return rlock_protocol::OK;
}

rlock_protocol::status
lock_client_cache_rsm::retry_handler(lock_protocol::lockid_t lid, lock_protocol::xid_t, int &)
{
  ScopedLock ml(&m);

  std::map<lock_protocol::lockid_t, lock_t>::iterator it = locks.find(lid);

  if (it == locks.end()) {
    tprintf("lock %lld not found.\n", lid);
    return rlock_protocol::RPCERR;
  }

  it->second.should_retry = true;
  pthread_cond_signal(&it->second.retry_c);

  return rlock_protocol::OK;
}
