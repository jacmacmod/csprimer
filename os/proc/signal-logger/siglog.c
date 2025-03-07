#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

volatile uint64_t handled = 0;
sigjmp_buf jmp_env_seg, jmp_env_bus;

void handle(int sig) {
  handled |= (1 << sig);
  printf("Caught %d: %s (%d total)\n", sig, sys_siglist[sig],
         __builtin_popcount(handled));

  switch (sig) {
    case SIGINT:
    case SIGQUIT:
    case SIGTERM:
      exit(0);
    case SIGSEGV:
      siglongjmp(jmp_env_seg, 1);
    case SIGBUS:
      siglongjmp(jmp_env_bus, 1);
    }
}

int main(int argc, char* argv[]) {
    // Register all valid signals
    for (int i = 0; i < NSIG; i++) {
        signal(i, handle);
    }
    // trigger alarm in 1 sec
    alarm(1);
 
    // create a child process and immediately exit
    if (0 == fork()) {
      exit(0);
    }
    // fault is theoritically recoverable
    // cause a segfault
    int *p = 0;
    if (sigsetjmp(jmp_env_seg, 1) == 0) {
      *p = 5;
    }

    int *q;
    if (sigsetjmp(jmp_env_bus, 1) == 0) {
      *q = 7;
    }

    
    // spin
    for (;;)
      sleep(1);
}
