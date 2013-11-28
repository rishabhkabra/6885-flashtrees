
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "statuscodes.h"
#include "types.h"

extern int NAND_PAGE_SIZE;
extern int NAND_ERASE_SIZE;


#ifndef FLASH_H
#define FLASH_H

#define FLASH_BV

// Caller accesses

#define FLASH_MISC -1
#define FLASH_LATREE_NODE 1

#define LATREE_NODE_INDEX 0

#define N_IDX 1 //keep in sync with above

#define OP_READ 0
#define OP_WRITE 1
#define OP_ERASE 2

#define N_OPER 3 //write, read, erase

/* We should not have these seperate statically defined partitions -- The
 * partitions should be returned by the flash dynamically */

#define BUFFER_VOLUME 0
#define NODE_VOLUME 1
#define OTHER_VOLUME 2
#define NODE_SHADOW 3
#define MAIN_NUM_PARTITIONS 4

#define MAX_FLASH_PARTITIONS (MAIN_NUM_PARTITIONS)

extern bool IN_GC;
extern bool IN_GC_SEARCHING;

extern double whichCost[N_IDX][N_OPER];
extern long long whichNumCost[N_IDX][N_OPER];
extern long long whichByteCost[N_IDX][N_OPER];

#define MAX_FLASH_MEM_SEGMENTS (2048) 
#define FLASH_SEGMENT_SIZE (32 << 20) //Max: 64GB
#define FLASH_INITIAL_NUM_SEGMENTS (2) //64MB initial memory
#define SEGMENTS_TO_ALLOCATE_AT_TIME (2) //64MB extra memory

struct PartitionInfo_t{
	long long num_writes;
	long long num_reads;
	long long num_erases;
	double readCost, writeCost, eraseCost;
	long long byteSize;
	long long actualPages;
	long long npages;
#ifdef REAL_FILE
	int fd;
#else
	int numSegmentsUsed;
	unsigned char* memory[MAX_FLASH_MEM_SEGMENTS];
#endif
};

// Functions

int  FlashInit();

int FlashDestroy();

int  FlashWrite(bool doRealIo, int partition, int callerId, pageptr_t page, offsetptr_t offset, void *data, offsetptr_t len, int hint);

int  FlashRead(bool doRealIo, int partition, int callerId, pageptr_t page, offsetptr_t offset, void *data, offsetptr_t len, int hint);

int  FlashErase(bool doRealIo, int partition, int callerId, pageptr_t page, int hint);

int FlashDump(FILE* f);

void register_partition(int partitionNumber, int numPages);

void FlashZeroCounters();

double traceDump(int partition, int callerId, int oper, offsetptr_t len, int hint);

void accountFlashCost(int callerId, int oper, double cost, int hint, int partition, int ntimes, int nbytes);

int FlashEraseRegion(bool doRealIo, int partition, int callerId, pageptr_t startPage, int hint, int numberOfPages);
double calculateFlashCost(int oper, int len, int* givenBytes, int* givenTimes);
void flashCostPageWritten(int callerId, int npages = 1);

extern double FLASH_FIXED_WRITE_COST;
extern double FLASH_FIXED_READ_COST;
extern double FLASH_VARIABLE_WRITE_COST;
extern double FLASH_VARIABLE_READ_COST;
extern double FLASH_ERASE_COST;
extern double FLASH_RANDOM_WRITE_FIXED_COST;
extern double FLASH_RANDOM_WRITE_VAR_COST;

void flashLoadCosts(char* fileName);
void copy_partition(int tovol, int fromvol);
/* Structure specific accounting hint flags */

double FLASH_WRITE_COST(int len);
double FLASH_READ_COST(int len);
void flash_close(int partNumber);

#define PAGE_WRITE_COST (FLASH_WRITE_COST(NAND_PAGE_SIZE))
#define PAGE_READ_COST (FLASH_READ_COST(NAND_PAGE_SIZE))

#define zeroedDataBufLen (16384)
extern unsigned char zeroedDataBuf[zeroedDataBufLen];

#endif
