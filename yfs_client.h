#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>
#include <random>
#include <memory>

#include "lock_protocol.h"
#include "lock_client.h"

class yfs_client {
 private:
  extent_client *ec;
  lock_client *lc;

  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution;

 public:
  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  std::string filename(inum);
  inum n2i(std::string);
  inum new_inum(bool);

 public:
  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  status getfile(inum, fileinfo &);
  status getdir(inum, dirinfo &);

  status read(inum, size_t, off_t, std::string &);
  status write(inum, const char *, size_t, off_t);
  status setattr(inum, size_t);  // Only set size.

  status readdir(inum, std::vector<dirent> &);
  status lookup(inum, const char *, inum &);
  status create(inum, bool, const char *, inum &);

  status unlink(inum, const char *);
};

#endif
