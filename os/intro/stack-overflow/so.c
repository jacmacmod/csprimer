#include <stdio.h>
#include <unistd.h>
#include <sys/resource.h>

void fn(int n, long int bottom) {
	if (n % 10000 == 0) {
		fprintf(stderr, "[pid %d] frame %d %ld (%p)\n",getpid(), n, bottom - (long)&n, &n);
	}
	fn(n + 1, bottom);
}

void start() {
	int depth = 0;
	fn(depth, (long) &depth);
}
int main() {
	struct rlimit rl;
	getrlimit(RLIMIT_STACK, &rl);
	printf("Current max: %llu, hard limit %llu\n", rl.rlim_cur, rl.rlim_max); 
	 rl.rlim_cur = rl.rlim_max;
	 setrlimit(RLIMIT_STACK, &rl);
	printf("Current max: %llu, hard limit %llu\n", rl.rlim_cur, rl.rlim_max); 
	start();
	printf("OK\n");
}
