#include "rpc.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#ifdef RSM
#include "lock_server_cache_rsm.h"
#else
#include "lock_server_cache.h"
#endif
#include "paxos.h"
#include "rsm.h"

#include "jsl_log.h"

// Main loop of lock_server

static void
force_exit(int) {
  exit(0);
}

int
main(int argc, char *argv[])
{
  int count = 0;

  // Force the lock_server to exit after 20 minutes
  signal(SIGALRM, force_exit);
  alarm(20 * 60);

  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);

  srandom(getpid());

#ifdef RSM
  if (argc != 3) {
    fprintf(stderr, "Usage: %s [master:]port [me:]port\n", argv[0]);
    if (argc == 2) {
      fprintf(stderr, "Only one port is specified, fall back to single node mode.\n");
      argv[2] = argv[1];
      argc = 3;
    } else {
      exit(1);
    }
  }
#else
  if (argc != 2) {
    fprintf(stderr, "Usage: %s port\n", argv[0]);
    exit(1);
  }
#endif

  char *count_env = getenv("RPC_COUNT");
  if (count_env != NULL) {
    count = atoi(count_env);
  }

  // jsl_set_debug(2);
  // Comment out the next line to switch between the ordinary lock
  // server and the RSM.  In Lab 6, we disable the lock server and
  // implement Paxos.  In Lab 7, we will make the lock server use your
  // RSM layer.
#ifdef RSM
// You must comment out the next line once you are done with Step One.
#define STEP_ONE
#ifdef STEP_ONE
  rpcs server(atoi(argv[1]));
  lock_server_cache_rsm ls;
  server.reg(lock_protocol::acquire, &ls, &lock_server_cache_rsm::acquire);
  server.reg(lock_protocol::release, &ls, &lock_server_cache_rsm::release);
  server.reg(lock_protocol::stat, &ls, &lock_server_cache_rsm::stat);
#else
  rsm rsm(argv[1], argv[2]);
  lock_server_cache_rsm ls(&rsm);
  rsm.set_state_transfer((rsm_state_transfer *) &ls);
  rsm.reg(lock_protocol::acquire, &ls, &lock_server_cache_rsm::acquire);
  rsm.reg(lock_protocol::release, &ls, &lock_server_cache_rsm::release);
  rsm.reg(lock_protocol::stat, &ls, &lock_server_cache_rsm::stat);
#endif // STEP_ONE
#endif // RSM

#ifndef RSM
  lock_server_cache ls;
  rpcs server(atoi(argv[1]), count);
  server.reg(lock_protocol::stat, &ls, &lock_server_cache::stat);
  server.reg(lock_protocol::acquire, &ls, &lock_server_cache::acquire);
  server.reg(lock_protocol::release, &ls, &lock_server_cache::release);
#endif

  while (true) {
    sleep(1000);
  }

  return 0;
}
