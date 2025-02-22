#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>

void print_stats(struct stat buf) {
	printf("inode #: %llu blocks: %lld block_size: %d size: %lld\n", 
		buf.st_ino, buf.st_blocks, buf.st_blksize, buf.st_size);
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("must provide path to file\n");
		exit(1);
	}
	
	int fd, blks;	
	blks = 0;
	struct stat buf;
	char buffer[1];
	buffer[0] = 'P';
	
	if ((fd = open(argv[1], O_WRONLY | O_APPEND)) < 0) {
		perror("open");
		exit(1);
	}

	for (int i = 0; i < 1048576; i++) {
		if (write(fd, buffer, 1) < 1) {
			close(fd);
			perror("write");
			exit(1);
		}
		if (stat(argv[1], &buf) < 0) {
			perror("stat");
			close(fd);
			exit(1);
		}
		if (blks != buf.st_blocks || i == 0) {
			blks = buf.st_blocks; 
			printf("block count --> ");
			print_stats(buf);
		}
	}		

	close(fd);
	printf("final stats --> ");	
	print_stats(buf);
	printf("done\n");
}

