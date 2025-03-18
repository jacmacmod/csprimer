#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROMPT "🥥_ "
#define MAXCMD 4096
#define MAXARGV 256
#define SEP " \t\n"

volatile pid_t childpid = 0;

void sigint_handler(int sig) {
  if (!childpid)
    return;
  if (kill(childpid, SIGINT) < 0)
    perror("Error sending sigint to child");
  return;
}

int main() {
  int rc;
  char input[MAXCMD];
  char *argv[MAXARGV];

  signal(SIGINT, sigint_handler);

  while (1) {
    printf(PROMPT);
    if ((NULL == fgets(input, MAXCMD, stdin)) && ferror(stdin)) {
      perror("fgets error");
      exit(1);
    }

    if (feof(stdin))
      exit(0);

    if (input[strlen(input) - 1] == '\n')
      input[strlen(input) - 1] = 0;

    char *pch;
    int argc = 0;
    pch = argv[argc] = strtok(input, SEP);
    while (argv[argc] != NULL)
      argv[++argc] = pch = strtok(NULL, SEP);

    argv[argc] = NULL;

    if (argc == 0)
      continue;

    if (strcmp(argv[0], "quit") == 0) {
      printf("good bye\n");
      exit(0);
    }

    if (strcmp(argv[0], "help") == 0) {
      printf("you have reached the help docs\n");
      continue;
    }

    if ((childpid = fork()) < 0) {
      perror("fork error");
    }

    if (childpid == 0) {
      if (execvp(argv[0], argv) < 0) {
        perror("exec error");
        exit(1);
      }

      exit(1);
    }

    int status;
    waitpid(childpid, &status, 0);
    childpid = 0;
  }
}
