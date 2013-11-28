#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <signal.h>

#include <sys/fcntl.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include "util.h"

#define ORIGINALBLOCKSIZE origBlkSize
#define BLOCKSIZE 8192
#define TIMEOUT 30

int numops = 10000;
int count;
time_t start;
int origBlkSize = 512;
char* disk = "/dev/sdb";
bool readAhead = false;
bool writeCaching = true;
bool doDirectIo = true;

long elapsed_utime;    /* elapsed time in microseconds */
long elapsed_seconds;  /* diff between seconds counter */
long elapsed_useconds; /* diff between microseconds counter */

void handle(const char *string, int error)
{
	if (error) {
		perror(string);
		exit(EXIT_FAILURE);
	}
}

int main(int argc, char **argv)
{
	//	char buffer[BLOCKSIZE];
	void* realbuffer;
	char* alignedbuffer;
	int fd, retval;
	unsigned long numblocks = 0;
	off64_t offset;

	setvbuf(stdout, NULL, _IONBF, 0);

	printf("Seeker v2.0, 2007-01-15, "
			"http://www.linuxinsight.com/how_fast_is_your_disk.html\n");

	alignedbuffer = (char*)getPageAlignedBuffer(BLOCKSIZE, &realbuffer);
	assert(alignedbuffer);
    char ch;
    while ((ch = getopt(argc, argv, "wrdt:o:")) != EOF){
        switch(ch) {
            case 'd':
                doDirectIo = false;
                fprintf(stderr, "\nNot doing direct io");
                break;
            case 't':
				disk = strdup(optarg);
				assert(disk);
                break;
            case 'o':
				origBlkSize = atoi(optarg);
				assert(origBlkSize % 512 == 0);
                break;
			case 'r':
                readAhead = true;
                fprintf(stderr, "\nDoing read ahead");
				break;
			case 'w':
                writeCaching = false;
                fprintf(stderr, "\nDisabling write back caching");
				break;
			default:
				fprintf(stderr, "\nIncorrect option ... %c", ch);
				exit(1);
			}
	}
	fprintf(stdout, "\nBenchmarking %s with %d ops, with block size %d:", disk, numops, origBlkSize);
	fprintf(stdout, "\nWith parameters: caching: %s, readahead: %s", writeCaching ? "Write-Back" : "Write-Through", readAhead ? "enabled" : "disabled");

	char cmd[255];

	snprintf(cmd, sizeof(cmd), "/sbin/blockdev --setra %d %s", readAhead ? 1 : 0, disk); 
	fprintf(stderr, "\nSetting read ahead %d ... %s\n", readAhead ? 1 : 0, cmd);
	system(cmd);
	sleep(1);

	snprintf(cmd, sizeof(cmd), "/sbin/hdparm -W %d %s", writeCaching ? 1 : 0, disk); 
	fprintf(stderr, "\nSetting read ahead %d ... %s\n", writeCaching ? 1 : 0, cmd);
	system(cmd);
	sleep(1);

	sleep(1);

	fd = open(disk, O_RDONLY);
	assert(fd >= 0);
	retval = ioctl(fd, BLKGETSIZE, &numblocks);
	assert(retval >= 0);
	close(fd);
	unsigned long orignumblocks = numblocks;
	numblocks=(numblocks*(unsigned long)(double(512)/double(ORIGINALBLOCKSIZE))) - 1;
	printf("no. of blocks are %lu (orig: %lu)\n\n",numblocks, orignumblocks);
	printf("Benchmarking %s [%luMB]\n", disk, numblocks / 2048);

	fd = open(disk, O_RDWR | (doDirectIo ? O_DIRECT : 0));
	assert(fd >= 0);

	int i, j,k;

	//Benchmark1 : Random reads

	double tStart;
	double tTaken;


	printf("\n\n ****** STARTING ********** \n\n");

	//-----------------------------------------------------
	//Benchmark1 : Random reads
	tStart = nowTime();
	for (i=0;i<numops;i++) 
	{
		offset = (off64_t) numblocks * random() / RAND_MAX;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = read(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Random Reads:: Elapsed time per = %lf microseconds\n", (tTaken * 1e6)/numops);



	//-----------------------------------------------------
	//Benchmark2 : Random writes
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = (off64_t) numblocks * random() / RAND_MAX;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Random Writes:: Elapsed time per = %lf microseconds\n", (tTaken * 1e6)/numops);


	//-----------------------------------------------------
	//Benchmark3 : Sequential reads
	retval = lseek64(fd, ORIGINALBLOCKSIZE * 1, SEEK_SET);
	handle("lseek64", retval == (off64_t) -1);
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = i;
		retval = read(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Sequential reads:: Elapsed time per = %lf microseconds\n", (tTaken * 1e6)/numops);

	//----------------------------------------------------------
	//Benchmark4 : Sequential writes
	retval = lseek64(fd, ORIGINALBLOCKSIZE * 1, SEEK_SET);
	handle("lseek64", retval == (off64_t) -1);
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = i;
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Sequential writes:: Elapsed time per = %lf microseconds\n", (tTaken * 1e6)/numops);

	//----------------------------------------------------------
	//Benchmark5 : Sequential writes interlaved with random reads
	retval = lseek64(fd, ORIGINALBLOCKSIZE * 1, SEEK_SET);
	handle("lseek64", retval == (off64_t) -1);
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = i;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		offset = (off64_t) numblocks * random() / RAND_MAX;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = read(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}

	tTaken = (nowTime() - tStart);
	printf("Sequential writes interleaved with Random reads:: Elapsed time per comined op %lf microseconds\n", (tTaken * 1e6)/numops);

	//----------------------------------------------------------
	//Benchmark6 : Sequential writes interlaved with sequential reads
	retval = lseek64(fd, ORIGINALBLOCKSIZE * 1, SEEK_SET);
	handle("lseek64", retval == (off64_t) -1);
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = i;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		offset = 2 * numops + i;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = read(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Sequential writes mixed with Sequential reads:: Elapsed time per comined op %lf microseconds\n", (tTaken * 1e6)/numops);


	//----------------------------------------------------------
	//Benchmark7 : Sequential writes interlaved with sequential writes
	retval = lseek64(fd, ORIGINALBLOCKSIZE * 1, SEEK_SET);
	handle("lseek64", retval == (off64_t) -1);
	tStart = nowTime();
	for (i=0;i<numops;i++)
	{
		offset = i;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		handle("write", retval < 0);
		offset = (off64_t) numblocks * random() / RAND_MAX;
		retval = lseek64(fd, ORIGINALBLOCKSIZE * offset, SEEK_SET);
		handle("lseek64", retval == (off64_t) -1);
		retval = write(fd, alignedbuffer, ORIGINALBLOCKSIZE);
		handle("write", retval < 0);
		count++;
	}
	tTaken = (nowTime() - tStart);
	printf("Sequential writes mixed with Sequential writes:: Elapsed time per comined op %lf microseconds\n", (tTaken * 1e6)/numops);
	
	//Done benchmarking
	close(fd);
	return 0;
}

