// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

lock_client_cache::lock_client_cache(
    std::string xdst, class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  id = std::string("127.0.0.1:") + std::to_string(rlsrpc->port());

  pthread_mutex_init(&m, NULL);
}

int
lock_client_cache::stat(lock_protocol::lockid_t lid)
{
  int r;
  cl->call(lock_protocol::stat, lid, id, r);
  return r;
}

lock_protocol::status
lock_client_cache::acquire_impl(
    lock_protocol::lockid_t lid,
    std::map<lock_protocol::lockid_t, lock_t>::iterator it)
{
  VERIFY(it->second.status == lock_status::none);

  it->second.status = lock_status::acquiring;

  lock_protocol::status ret;
  int r = 0;

  while (true) {
    pthread_mutex_unlock(&m);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&m);

    if (ret == lock_protocol::OK || ret != lock_protocol::RETRY) {
      break;
    }

    while (!it->second.should_retry) {
      pthread_cond_wait(&it->second.retry_c, &m);
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
lock_client_cache::release_impl(
    lock_protocol::lockid_t lid,
    std::map<lock_protocol::lockid_t, lock_t>::iterator it)
{
  lock_status status = it->second.status;

  VERIFY(status == lock_status::locked || status == lock_status::free);

  it->second.status = lock_status::releasing;

  if (lu != NULL) {
    lu->dorelease(lid);
  }

  lock_protocol::status ret;
  int r;

  pthread_mutex_unlock(&m);
  ret = cl->call(lock_protocol::release, lid, id, r);
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
lock_client_cache::acquire(lock_protocol::lockid_t lid)
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
lock_client_cache::release(lock_protocol::lockid_t lid)
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

  if (it->second.revoked) {
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

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, int &)
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
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, int &)
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
