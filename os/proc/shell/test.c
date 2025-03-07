#include <stdio.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  // char *line = NULL;
  // size_t linecap = 0;
  // ssize_t linelen;
  // while ((linelen = getdelim(&line, &linecap, 32, stdin)) > 0) {
  //   printf("\nline length: %ld\n", linelen);
  //   fwrite(line, linelen, 1, stdout);
  // }
  //
  //

  int rc = fork();

  if (rc < 0) {
      return -1;
  } else if (rc == 0) {
      printf("hello from child");
  } else {
      printf("🤢\n");
  }
}
