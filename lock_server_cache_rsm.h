#ifndef lock_server_cache_rsm_h
#define lock_server_cache_rsm_h

#include <string>
#include <queue>

#include "lock_protocol.h"
#include "rpc.h"
#include "rsm_state_transfer.h"
#include "rsm.h"
#include "uqueue.h"
#include "fifo.h"

class lock_server_cache_rsm : public rsm_state_transfer {
 private:
  class rsm *rsm;

  enum lock_status {
    free,    // server has the free lock
    lent,    // lock is lent to some client @owner
    revoked, // lock is lent but being revoked
  };

  struct lock_t {
    lock_status status;
    int nacquire;
    std::string owner;
    uqueue<std::string> queue;     // waiting list
    lock_protocol::xid_t last_xid; // xid of last execution

    lock_t() : status(lock_status::free), nacquire(0), last_xid(0) { }
  };
  std::map<lock_protocol::lockid_t, lock_t> locks;

  struct task_t {
    lock_protocol::lockid_t lid;
    std::string client;
  };
  fifo<task_t> revoke_tasks;
  fifo<task_t> retry_tasks;

  pthread_mutex_t m;

 public:
  lock_server_cache_rsm(class rsm *rsm = 0);

  void revoker();
  void retryer();

  std::string marshal_state();
  void unmarshal_state(std::string state);

  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(lock_protocol::lockid_t, std::string, lock_protocol::xid_t, int &);
  lock_protocol::status release(lock_protocol::lockid_t, std::string, lock_protocol::xid_t, int &);
};

#endif
