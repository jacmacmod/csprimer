#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define MAXINPUTSIZE 100
int main() {
  int rc;
  char input[MAXINPUTSIZE];
  char *cmd;
  char **args;

  printf("🥥_ ");
  while (fgets(input, MAXINPUTSIZE, stdin) != NULL) {
    if (input[strlen(input)] == '\n') {
      input[strlen(input) - 1] = 0;
    }

    int i = 0, argIdx = 0, size = 0, tmpArrIdx = 0, argc = 5, argSize = 50;
    char *tmpArr = malloc(argSize);
    int prev_c = input[0];

    args = malloc(argc);

    while (input[i] != '\0') {
      if (input[i] == '\n' || (input[i] == '\t' || input[i] == ' ') &&
                                  prev_c != ' ' && prev_c != '\t') {
        tmpArr[strlen(tmpArr)] = 0;
        args[argIdx] = tmpArr;
        if (argIdx == 0)
          cmd = tmpArr;
        size++, argIdx++, tmpArrIdx = 0;
        tmpArr = NULL;
        tmpArr = malloc(argSize);
      }
      tmpArr[tmpArrIdx] = input[i];
      prev_c = input[i], tmpArrIdx++, i++;
    }
    args[argIdx + 1] = NULL;

    if ((rc = fork()) < 0) {
      exit(1);
    } else if (rc == 0) {
      if (args[0] != NULL) {
        execvp(cmd, args);
      } else {
        execlp(cmd, cmd, (char *)0);
      }
      exit(1);
    } else {
      wait(NULL);
    }
    printf("\n🥥_ ");
  }
}

// while ((c = getchar()) != EOF) {
//     putchar(c);
//   if (c == '\n') {
//     printf("here %c\n", c);
//     // complete word and index
//     w[i][j] = '\0';
//     w[++i] = NULL;
//     printf("%s", w[0]);
//     i = 0, j = 0;
//     execvp(w[0], w);
//     *w = NULL;
//     printf("🥥_ ");
//   } else if (c == '\b' || c == '\t') {
//     w[i][j] = '\0';
//     i++;
//     j = 0;

//   } else {
//     w[i][j] = c;
//     j++;
//   }
// }
// exit(0);
