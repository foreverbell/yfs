#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <map>
#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "uqueue.h"

class lock_server_cache {
 private:
  enum lock_status {
    free,    // server has the free lock
    lent,    // lock is lent to some client @owner
    revoked, // lock is lent but being revoked
  };

  struct lock_t {
    lock_status status;
    int nacquire;
    std::string owner;
    uqueue<std::string> queue; // waiting list

    lock_t() : status(lock_status::free), nacquire(0) { }
  };
  std::map<lock_protocol::lockid_t, lock_t> locks;

  pthread_mutex_t m;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  // The return value indicates if there are other clients trying to acquire the lock.
  lock_protocol::status acquire(lock_protocol::lockid_t, std::string, int &);
  lock_protocol::status release(lock_protocol::lockid_t, std::string, int &);
};

#endif
