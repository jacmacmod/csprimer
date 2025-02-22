#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>

#define SLEEP_SEC 3
#define NUM_MULS 100000000
#define NUM_MALLOCS 10000000
#define MALLOC_SIZE 1000
#define TOTAL_USEC(tv) (tv).tv_sec * 1000000 + (tv).tv_usec
#define TO_SEC(usec) (usec) / 1000000.0

struct profile_times
{
  uint64_t real_usec;
  uint64_t user_usec;
  uint64_t system_usec;
};

void profile_start(struct profile_times *t)
{
  struct timeval tv;
  struct rusage ru;
  gettimeofday(&tv, NULL);
  getrusage(RUSAGE_SELF, &ru);
  t->real_usec = TOTAL_USEC(tv);
  t->user_usec = TOTAL_USEC(ru.ru_utime);
  t->system_usec = TOTAL_USEC(ru.ru_stime);
}

void profile_log(struct profile_times *t)
{
  struct timeval tv;
  struct rusage ru;
  gettimeofday(&tv, NULL);
  getrusage(RUSAGE_SELF, &ru);
  uint64_t real_diff = TOTAL_USEC(tv) - t->real_usec;
  uint64_t user_diff = TOTAL_USEC(ru.ru_utime) - t->user_usec;
  u_int64_t system_diff = TOTAL_USEC(ru.ru_stime) - t->system_usec;
  fprintf(stderr, "[pid: %d] real: %0.03f user: %0.03f system: %0.03f\n\n",
          getpid(), TO_SEC(real_diff), TO_SEC(user_diff), TO_SEC(system_diff));
}

int main(int argc, char *argv[])
{
  struct profile_times t;

  // TODO profile doing a bunch of floating point muls
  float x = 1.0;
  profile_start(&t);
  for (int i = 0; i < NUM_MULS; i++)
    x *= 1.1;
  profile_log(&t);

  // TODO profile doing a bunch of mallocs
  profile_start(&t);
  void *p;
  for (int i = 0; i < NUM_MALLOCS; i++)
    p = malloc(MALLOC_SIZE);
  profile_log(&t);

  // TODO profile sleeping
  profile_start(&t);
  sleep(SLEEP_SEC);
  profile_log(&t);

  printf("ok\n");
}
