// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sstream>
#include <iostream>
#include <random>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

// Pray that we have no collision!
yfs_client::inum
new_inum(bool is_file)
{
  static std::default_random_engine generator;
  static std::uniform_int_distribution<int> distribution(2, (1u << 31) - 1);

  return distribution(generator) | (is_file ? 0x80000000 : 0x0);
}

bool
yfs_client::isfile(inum inum)
{
  if (inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return !isfile(inum);
}

yfs_client::status
yfs_client::getfile(inum inum, fileinfo &fin)
{
  yfs_client::status r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  return r;
}

yfs_client::status
yfs_client::getdir(inum inum, dirinfo &din)
{
  yfs_client::status r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

yfs_client::status
yfs_client::read(inum inum, size_t size, off_t offset, std::string &output)
{
  if (!isfile(inum)) {
    return NOENT;
  }

  extent_protocol::status status;
  std::string buf;

  status = ec->get(inum, buf);
  if (status != OK) {
    return status;
  }

  // Adjust the size to read of fit the file.
  if (offset >= (off_t) buf.size()) {
    size = 0;
  } else {
    size = std::min(size, buf.size() - offset);
  }

  output = buf.substr(offset, size);

  return OK;
}

yfs_client::status
yfs_client::write(inum inum, const char *input, size_t size, off_t offset)
{
  if (!isfile(inum)) {
    return NOENT;
  }

  extent_protocol::status status;
  std::string buf;

  status = ec->get(inum, buf);
  if (status != OK) {
    return status;
  }

  // Grow the file if necessary.
  if (buf.size() < size + offset) {
    buf.resize(size + offset);
  }

  for (size_t i = 0; i < size; ++i) {
    buf[offset + i] = input[i];
  }

  return ec->put(inum, buf);
}

yfs_client::status
yfs_client::setattr(inum inum, size_t size)
{
  if (!isfile(inum)) {
    return NOENT;
  }

  extent_protocol::status status;
  std::string buf;

  status = ec->get(inum, buf);
  if (status != OK) {
    return status;
  }

  buf.resize(size);

  return ec->put(inum, buf);
}

//
// The directory content is stored in the following format:
//  /file_1/inum_1/file_2/inum_2/.../file_n/inum_n.
//

yfs_client::status
yfs_client::readdir(inum parent, std::vector<dirent> &ents)
{
  if (!isdir(parent)) {
    return NOENT;
  }

  extent_protocol::status status;
  std::string buf;

  status = ec->get(parent, buf);
  if (status != OK) {
    return status;
  }

  ents.clear();

  size_t last_slash = 0;

  while (true) {
    size_t slash_1, slash_2;

    slash_1 = buf.find("/", last_slash + 1);
    if (slash_1 == std::string::npos) {
      return IOERR;  // corrupted data
    }
    slash_2 = buf.find("/", slash_1 + 1);

    dirent ent;

    ent.name = buf.substr(last_slash + 1, slash_1 - last_slash - 1);
    ent.inum = n2i(buf.substr(slash_1 + 1, slash_2 - slash_1 - 1));

    ents.emplace_back(std::move(ent));

    if (slash_2 == std::string::npos) {
      break;
    } else {
      last_slash = slash_2;
    }
  }

  return OK;
}

// Return OK if found.
yfs_client::status
yfs_client::lookup(inum parent, const char *name, inum &child)
{
  std::vector<dirent> ents;
  yfs_client::status status;

  status = readdir(parent, ents);
  if (status != OK) {
    return status;
  }

  for (std::vector<dirent>::iterator it = ents.begin(); it != ents.end(); ++it) {
    if (it->name == name) {
      child = it->inum;
      return OK;
    }
  }

  return NOENT;
}

yfs_client::status
yfs_client::create(inum parent, bool is_file, const char *name, inum &child)
{
  if (!isdir(parent)) {
    return NOENT;
  }

  extent_protocol::status status;
  std::string buf;

  status = ec->get(parent, buf);
  if (status != OK) {
    return status;
  }

  if (buf.find(std::string("/") + name + "/") != std::string::npos) {
    return EXIST;
  }

  child = new_inum(is_file);

  status = ec->put(child, "");
  if (status != OK) {
    return status;
  }

  buf.append("/");
  buf.append(name);
  buf.append("/");
  buf.append(filename(child));

  // XXX: concurrent put
  status = ec->put(parent, buf);
  if (status != OK) {
    return status;
  }

  return OK;
}
