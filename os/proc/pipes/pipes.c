#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PROMPT "🥥_ "
#define MAXCMD 4096
#define MAXARGV 8
#define SEP " \t\n"

volatile pid_t pid = 0;

    void sigint_handler(int sig)
{
  if (!pid)
    return;
  if (kill(pid, SIGINT) < 0)
    perror("Error sending sigint to child");
  return;
}

int main()
{
      int rc;
  char input[MAXCMD];
  char *argv[MAXARGV];
  char *cmds[MAXCMD][MAXARGV];
  signal(SIGINT, sigint_handler);

  while (1)
  {
    printf(PROMPT);
    if ((NULL == fgets(input, MAXCMD, stdin)) && ferror(stdin))
    {
      perror("fgets error");
      exit(1);
    }

    if (feof(stdin))
      exit(0);

    input[strlen(input) - 1] = '\0';

    char *pch;
    char *inputp = input;
    int cmdi = 0, argi = 0;
    while (1)
    {
      pch = strsep(&inputp, SEP);
      if (pch == NULL || *pch == '\0')
      {
        cmds[cmdi][argi] = NULL;
        break;
      }
      if (strcmp(pch, "|") == 0)
      {
        cmds[cmdi++][argi] = NULL;
        argi = 0;
      }
      else
      {
        cmds[cmdi][argi++] = pch;
      }
    }

    if (argi == 0)
      continue;

    if (strcmp(cmds[0][0], "quit") == 0)
    {
      printf("good bye\n");
      exit(0);
    }

    if (strcmp(cmds[0][0], "help") == 0)
    {
      printf("you have reached the help docs\n");
      continue;
    }

    int filedes[2];
    int pidssize = cmdi + 1;
    int pids[pidssize];
    int readfiledes = STDIN_FILENO;

    for (int i = 0; i <= cmdi; i++)
    {
      if (cmdi > 0 && i < cmdi)
      {
        printf("pipe created\n");
        if (pipe(filedes) < 0)
        {
          perror("pipe error");
          exit(1);
        }
      }

      if ((pid = pids[i] = fork()) < 0)
        perror("fork error");

      if (pids[i] == 0)
      {
        printf("child i: %d, pid: %d\n", i, pids[i]);
        if (i < cmdi)
        {
          // not last child A | ... | LAST
          // read from parent STDOUT file descriptor
          dup2(filedes[1], STDOUT_FILENO);
          close(filedes[1]);
          close(filedes[0]);
        }
        if (i != 0)
          dup2(readfiledes, STDIN_FILENO);
        if (execvp(cmds[i][0], cmds[i]) < 0)
        {
          perror("exec error");
          exit(1);
        }
        exit(1);
      }

      // parent
      printf("parent i: %d, pid: %d infd %d\n", i, pids[i], readfiledes);
      if (i != cmdi)
      {
        if (readfiledes != STDIN_FILENO)
          close(readfiledes);

        readfiledes = filedes[0];
        close(filedes[1]);
      }
    }

    if (readfiledes != 0)
      close(readfiledes);

    int status;
    for (int i = 0; i <= cmdi; i++)
      waitpid(pids[i], &status, 0);
  }
}


