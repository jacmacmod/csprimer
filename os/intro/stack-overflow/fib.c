#include <stdio.h>

int fib(int n) {
	if (n <= 1) return n;
	int left = fib(n - 1);
	int right = fib(n - 2);
	return left + right;
}

int main() {
	int n = 500;
	printf("fib(%d) is %d\n", n, fib(n));
}
