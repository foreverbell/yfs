// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent.

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::get_impl(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret;

  std::string buf;
  extent_protocol::attr attr;

  ret = cl->call(extent_protocol::get, eid, buf);
  if (ret != extent_protocol::OK) {
    return ret;
  }
  ret = cl->call(extent_protocol::getattr, eid, attr);
  if (ret != extent_protocol::OK) {
    return ret;
  }

  extent_client::extent_t &ext = exts_cache[eid];

  ext.ext = buf;
  ext.attr = attr;
  ext.dirty = false;
  ext.removed = false;

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::put_impl(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret;
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;
  int r;

  it = exts_cache.find(eid);
  VERIFY(it != exts_cache.end() && !it->second.removed && it->second.dirty);

  ret = cl->call(extent_protocol::put, eid, std::move(it->second.ext), r);
  if (ret != extent_protocol::OK) {
    return ret;
  }

  exts_cache.erase(it);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove_impl(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret;
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;
  int r;

  it = exts_cache.find(eid);
  VERIFY(it != exts_cache.end() && it->second.removed);

  ret = cl->call(extent_protocol::remove, eid, r);
  if (ret != extent_protocol::OK) {
    return ret;
  }

  exts_cache.erase(it);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;

  it = exts_cache.find(eid);
  if (it != exts_cache.end()) { // Cache hit
    if (it->second.removed) {
      return extent_protocol::IOERR;
    }
    it->second.attr.atime = time_since_epoch();
    buf = it->second.ext;

    return extent_protocol::OK;
  }

  extent_protocol::status ret = get_impl(eid);
  if (ret != extent_protocol::OK) {
    return ret;
  }

  exts_cache[eid].attr.atime = time_since_epoch();
  buf = exts_cache[eid].ext;

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  std::map<extent_protocol::extentid_t, extent_t>::iterator it;

  it = exts_cache.find(eid);
  if (it != exts_cache.end()) { // Cache hit
    if (it->second.removed) {
      return extent_protocol::IOERR;
    }
    attr = it->second.attr;

    return extent_protocol::OK;
  }

  extent_protocol::status ret = get_impl(eid);
  if (ret != extent_protocol::OK) {
    return ret;
  }

  attr = exts_cache[eid].attr;

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_t ext;
  unsigned int t = time_since_epoch();

  ext.attr.size = buf.size();
  ext.attr.atime = t;
  ext.attr.mtime = t;
  ext.attr.ctime = t;
  ext.ext = std::move(buf);
  ext.dirty = true;
  ext.removed = false;

  exts_cache[eid] = std::move(ext);

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  exts_cache[eid].removed = true;

  return extent_protocol::OK;
}

extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  // TODO: Implementation.
  return extent_protocol::OK;
}
