// the extent server implementation

#include "extent_server.h"
#include <chrono>
#include <fcntl.h>
#include <sstream>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static unsigned int time() // <ctime> time(NULL);
{
  return std::chrono::system_clock::now().time_since_epoch() / std::chrono::seconds(1);
}

extent_server::extent_server()
{
  pthread_mutex_init(&m, NULL);

  // FUSE assumes that the inum for the root directory is 0x00000001.
  int r;
  put(1, "", r);
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("put request id=%lld, size=%ld\n", id, buf.size());

  ScopedLock ml(&m);
  extent_t ext;
  unsigned int t = time();

  ext.attr.size = buf.size();
  ext.attr.atime = t;
  ext.attr.mtime = t;
  ext.attr.ctime = t;
  ext.ext = std::move(buf);

  exts[id] = std::move(ext);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("get request id=%lld\n", id);

  ScopedLock ml(&m);
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;

  it = exts.find(id);
  if (it == exts.end()) {
    return extent_protocol::IOERR;
  }

  it->second.attr.atime = time();
  buf = it->second.ext;

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("getattr request id=%lld\n", id);

  ScopedLock ml(&m);
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;

  it = exts.find(id);
  if (it == exts.end()) {
    return extent_protocol::IOERR;
  }

  a = it->second.attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("remove request id=%lld\n", id);

  ScopedLock ml(&m);
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;

  it = exts.find(id);
  if (it == exts.end()) {
    return extent_protocol::IOERR;
  }

  exts.erase(it);

  return extent_protocol::OK;
}
