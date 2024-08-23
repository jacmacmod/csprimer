#include <assert.h>
#include <stdio.h>

int bitcount(unsigned int n);

int main() {
    assert(bitcount(0) == 0);
    assert(bitcount(1) == 1);
    assert(bitcount(3) == 2);
    assert(bitcount(0x0f) == 4);
    assert(bitcount(8) == 1);
    // harder case:
    assert(bitcount(0xffffffff) == 32);
    printf("OK\n");
}

int bitcount(unsigned int n) {

	int count = 0;
	int shift = 0;

	while (shift < 32) {
	   unsigned int shifted = 1 << shift;
		if ((n & shifted) > 0) {
			count += 1;
		}
		if (shifted > n) {
			break;
		}
		shift +=1;
	}
	return count;
}
