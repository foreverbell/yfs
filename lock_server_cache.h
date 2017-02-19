#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <map>
#include <queue>
#include <string>
#include <unordered_set>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

template <typename T>
class uqueue { // queue with unique elements
 private:
  std::queue<T> q;
  std::unordered_set<T> h;

 public:
  bool push(const T &x) {
    if (h.count(x)) {
      return false;
    }
    q.push(x);
    h.insert(x);
    return true;
  }

  T front() const {
    return q.front();
  }

  void pop() {
    T r = q.front();
    h.erase(r);
    q.pop();
  }

  bool empty() const {
    return q.empty();
  }
};

class lock_server_cache {
 private:
  enum lock_status {
    free,    // server has the free lock
    lent,    // lock is lent to some client @owner.
    revoked, // lock is lent but being revoked.
  };

  struct lock_t {
    lock_status status;
    int nacquire;
    std::string owner;
    uqueue<std::string> queue;

    lock_t() : status(lock_status::free), nacquire(0) { }
  };
  std::map<lock_protocol::lockid_t, lock_t> locks;

  pthread_mutex_t m;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(lock_protocol::lockid_t, std::string id, int &);
  lock_protocol::status release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
