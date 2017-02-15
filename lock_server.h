// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {
 protected:
  enum lock_status {
    free,
    locked
  };

  struct lock_t {
    int nacquire;
    lock_status status;
    pthread_cond_t free_c;

    lock_t()
      : nacquire(0),
        status(lock_status::free),
        free_c(PTHREAD_COND_INITIALIZER) { }
  };

  pthread_mutex_t m;

  std::map<lock_protocol::lockid_t, lock_t> locks;

 public:
  lock_server();
  ~lock_server() { }
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 
