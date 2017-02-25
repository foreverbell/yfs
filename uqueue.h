// queue with unique elements.

#ifndef uqueue_h
#define uqueue_h

#include <queue>
#include <unordered_set>
#include "rpc/marshall.h"

template <typename T>
class uqueue {
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

  std::queue<T> get() const {
    return q;
  }
};

template <typename T>
inline marshall &
operator<<(marshall &m, uqueue<T> q)
{
  std::queue<T> _q = q.get();

  m << (int) _q.size();
  while (!_q.empty()) {
    T elem = _q.front();
    _q.pop();
    m << elem;
  }
  return m;
}

template <typename T>
inline unmarshall &
operator>>(unmarshall &u, uqueue<T> &q)
{
  int size;

  u >> size;
  while (size > 0) {
    size -= 1;
    T elem;
    u >> elem;
    q.push(elem);
  }
  return u;
}

#endif
