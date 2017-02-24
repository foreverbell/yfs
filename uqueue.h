// queue with unique elements.

#ifndef uqueue_h
#define uqueue_h

#include <queue>
#include <unordered_set>

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
};

#endif
