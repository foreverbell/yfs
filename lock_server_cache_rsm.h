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

  struct acquire_reply_t {
    lock_protocol::status status;
    int ret;
    std::string revoke;
  };

  struct release_reply_t {
    lock_protocol::status status;
  };

  struct client_context_t {
    lock_protocol::xid_t last_xid;
    acquire_reply_t acquire_reply;
    release_reply_t release_reply;

    client_context_t() : last_xid(0) { }

    friend inline marshall& operator<<(marshall &m, client_context_t ctx) {
      m << ctx.last_xid;
      m << ctx.acquire_reply.status << ctx.acquire_reply.ret << ctx.acquire_reply.revoke;
      m << ctx.release_reply.status;
      return m;
    }

    friend inline unmarshall& operator>>(unmarshall &u, client_context_t &ctx) {
      u >> ctx.last_xid;
      u >> ctx.acquire_reply.status >> ctx.acquire_reply.ret >> ctx.acquire_reply.revoke;
      u >> ctx.release_reply.status;
      return u;
    }
  };

  struct lock_t {
    lock_status status;
    std::string owner;
    uqueue<std::string> wait_q; // waiting list
    std::map<std::string, client_context_t> client_ctx;

    lock_t() : status(lock_status::free) { }
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

  lock_protocol::status acquire(lock_protocol::lockid_t, std::string, lock_protocol::xid_t, int &);
  lock_protocol::status release(lock_protocol::lockid_t, std::string, lock_protocol::xid_t, int &);
};

#endif
