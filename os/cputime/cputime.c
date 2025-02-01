#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/times.h>

#define SLEEP_SEC 3
#define NUM_MULS 1000000000
#define NUM_MALLOCS 1000000
#define MALLOC_SIZE 1000

struct profile_times
{
  struct tms tmstart;
  struct tms tmsend;
  clock_t start;
  clock_t end;
  long pid;
  char *msg;
};

void profile_start(struct profile_times *t, char *msg)
{
  t->start = times(&t->tmstart);
  t->pid = (long) getpid();
  t->msg = msg;
}

void profile_log(struct profile_times *t)
{
  t->end = times(&t->tmsend);
  clock_t real = t->end - t->start;
  static long clock_tick = 0;
  if (clock_tick == 0)
    if ((clock_tick = sysconf(_SC_CLK_TCK)) < 0)
    {
      printf("clock tick error");
    }

  printf("pid %ld ", t->pid);
  printf("%s\n", t->msg);
  printf("real: %5.3fs ", real / (double) clock_tick);
  printf("user: %5.3fs ", (t->tmsend.tms_utime - t->tmstart.tms_utime)/ (double) clock_tick);
  printf("system: %5.3fs\n",  (t->tmsend.tms_stime - t-> tmstart.tms_stime) / (double) clock_tick);
}

int main(int argc, char *argv[])
{
  struct profile_times t;
  char msg[50];

  // TODO profile doing a bunch of floating point muls
  float x = 1.0;
  sprintf(msg, "floating point %d muls", NUM_MULS);
  profile_start(&t, msg);
  for (int i = 0; i < NUM_MULS; i++)
    x *= 1.1;
  profile_log(&t);

  // TODO profile doing a bunch of mallocs
  sprintf(msg, "do %d mallocs", NUM_MALLOCS);
  profile_start(&t, msg);
  void *p;
  for (int i = 0; i < NUM_MALLOCS; i++)
    p = malloc(MALLOC_SIZE);
  profile_log(&t);

  // TODO profile sleeping
  sprintf(msg, "sleep %d", SLEEP_SEC);
  profile_start(&t, msg);
  sleep(SLEEP_SEC);
  profile_log(&t);
}
