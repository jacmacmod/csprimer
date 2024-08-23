#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>


bool ispangram(char *s) {
  int count = 0;
  char alphabet[26];
  int i = 0;

  while (s[i] != '\0' && s[i] != '\n' && count != 26) {
    if (isalpha(s[i])) {
        int lowercaseChar = tolower(s[i]);
        if (alphabet[lowercaseChar - 97] != lowercaseChar) {
            alphabet[lowercaseChar - 97] = lowercaseChar;
            count ++;
        }
    }
    i++;
  }

  if (count == 26) {
      return true;
  }
  return false;
}

int main() {
  size_t len;
  ssize_t read;
  char *line = NULL;
  while ((read = getline(&line, &len, stdin)) != -1) {
    if (ispangram(line))
      printf("%s", line);
  }

  if (ferror(stdin))
    fprintf(stderr, "Error reading from stdin");

  free(line);
  fprintf(stderr, "ok\n");
}
