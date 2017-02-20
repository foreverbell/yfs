// lock client interface.

#ifndef lock_client_cache_h
#define lock_client_cache_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() { }
};

class lock_client_cache : public lock_client {
 private:
  enum lock_status {
    none,      // client knows nothing about this lock
    free,      // client owns the lock and no thread has it
    locked,    // client owns the lock and a thread has it
    acquiring, // the client is acquiring ownership
    releasing, // the client is releasing ownership
  };

  struct lock_t {
    lock_status status;

    bool revoked;
    bool should_retry;

    pthread_cond_t free_c;  // notify when status == lock_status::free || none
    pthread_cond_t retry_c; // notify when should_retry == true

    pthread_t owner;        // thread id of owner

    lock_t()
      : status(lock_status::none),
        revoked(false), should_retry(false), owner(0) {
          pthread_cond_init(&free_c, NULL);
          pthread_cond_init(&retry_c, NULL);
    }
  };

  class lock_release_user *lu;
//  int rlock_port;
//  std::string hostname;
  std::string id;
  std::map<lock_protocol::lockid_t, lock_t> locks;

  pthread_mutex_t m;

 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() { }

  int stat(lock_protocol::lockid_t);
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, int &);

  const char *id_() const { return id.c_str(); } // XXX: remove

 private:
  lock_protocol::status acquire_impl(
      lock_protocol::lockid_t,
      std::map<lock_protocol::lockid_t, lock_t>::iterator);
  lock_protocol::status release_impl(
      lock_protocol::lockid_t lid,
      std::map<lock_protocol::lockid_t, lock_t>::iterator);
};

#endif
