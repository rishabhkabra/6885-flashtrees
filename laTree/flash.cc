#include "flash.h"
#include "statuscodes.h"
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

double FLASH_FIXED_WRITE_COST;
double FLASH_FIXED_READ_COST;
double FLASH_VARIABLE_WRITE_COST;
double FLASH_VARIABLE_READ_COST;
double FLASH_ERASE_COST;
double FLASH_RANDOM_WRITE_FIXED_COST;
double FLASH_RANDOM_WRITE_VAR_COST;

enum iodir_t{FLASH_READ_DIR = 0, FLASH_WRITE_DIR};
int NAND_PAGE_SIZE = 512;
int NAND_ERASE_SIZE = 32;
char realFileName[1024];	
unsigned char zeroedDataBuf[zeroedDataBufLen];

double FLASH_WRITE_COST(int len){
	assert(len >= 0 && len <= NAND_PAGE_SIZE);
	return FLASH_FIXED_WRITE_COST + len * FLASH_VARIABLE_WRITE_COST;
}

double FLASH_READ_COST(int len){
	assert(len >= 0 && len <= NAND_PAGE_SIZE);
	return FLASH_FIXED_READ_COST + len * FLASH_VARIABLE_READ_COST;
}

// Global vars for this file
double whichCost[N_IDX][N_OPER];
long long whichNumCost[N_IDX][N_OPER];
long long whichByteCost[N_IDX][N_OPER];
long long numPagesWritten[N_IDX];
bool IN_GC = false;
bool IN_GC_SEARCHING = false;

bool TEMP_FLASH_FILL_INIT = false;

PartitionInfo_t partitions[MAX_FLASH_PARTITIONS];

unsigned char* FillBuffer = NULL;

off_t FLASH_OFFSET(pageptr_t page, offsetptr_t offset){
	off_t desiredLoc = ((off_t)page * (off_t)NAND_PAGE_SIZE) +  (off_t)offset;
	assert(desiredLoc >= 0);
	return desiredLoc;
}

void expand_flash_memory(int partition, int nsegs){
	int i;
	assert(partitions[partition].numSegmentsUsed + nsegs <= MAX_FLASH_MEM_SEGMENTS);
	for(i = 0; i < nsegs; i++){
		int j = i + partitions[partition].numSegmentsUsed;
		assert(!partitions[partition].memory[j]);
		partitions[partition].memory[j] = (unsigned char*)(malloc(FLASH_SEGMENT_SIZE));
		assert(partitions[partition].memory[j]);
		partitions[partition].byteSize += FLASH_SEGMENT_SIZE;
	}
	partitions[partition].numSegmentsUsed += nsegs;
}

void initialize_flash_memory(int partitionNumber){
	partitions[partitionNumber].byteSize = 0;	
	partitions[partitionNumber].numSegmentsUsed = 0;
	expand_flash_memory(partitionNumber, FLASH_INITIAL_NUM_SEGMENTS);
}

void free_up_flash_memory(int partitionNumber){
	int i;
	for(i = 0; i < partitions[partitionNumber].numSegmentsUsed; i++){
		assert(partitions[partitionNumber].memory[i]);
		free(partitions[partitionNumber].memory[i]);
		partitions[partitionNumber].memory[i]= NULL;
	}
	partitions[partitionNumber].byteSize = 0;
	partitions[partitionNumber].numSegmentsUsed = 0;
}

void do_flash_io(int partition, iodir_t dir, off_t desiredLoc, unsigned char* data, offsetptr_t len){
	assert( ((long long)(desiredLoc + len)) <= partitions[partition].byteSize);
	PartitionInfo_t* p = &partitions[partition];

	offsetptr_t remaining = len;
	offsetptr_t offset = 0;
	off_t loc = desiredLoc;
	do{
		off_t segmentNum = loc/FLASH_SEGMENT_SIZE;
		assert(segmentNum >= 0);
		assert(segmentNum < p->numSegmentsUsed);
		off_t startOffset = loc % FLASH_SEGMENT_SIZE;
		int toCopy = FLASH_SEGMENT_SIZE - startOffset;
		if (toCopy > remaining) toCopy = remaining;
		assert(toCopy <= FLASH_SEGMENT_SIZE);

		unsigned char* flashSegment = p->memory[segmentNum];
		assert(flashSegment);

		if (dir == FLASH_READ_DIR){
			memcpy(data + offset, flashSegment + startOffset, toCopy);
		}else{
			assert(dir == FLASH_WRITE_DIR);
			memcpy(flashSegment + startOffset, data + offset, toCopy);
		}

		offset+= toCopy;
		remaining -= toCopy;
		assert(remaining >= 0);
		loc += toCopy;
	}while(remaining > 0);
	assert(loc == desiredLoc + len);
}

void copy_flash_memory_partitions(int tovol, int fromvol){
	PartitionInfo_t* s = &partitions[fromvol];	
	PartitionInfo_t* d = &partitions[tovol];	
	int i;
	for(i = 0; i < s->numSegmentsUsed; i++){
		assert(i <= d->numSegmentsUsed); /* At max one bigger */
		if (i == d->numSegmentsUsed){
			expand_flash_memory(tovol, SEGMENTS_TO_ALLOCATE_AT_TIME);
		}
		assert(i < d->numSegmentsUsed); /* At max one bigger */
		memcpy(d->memory[i], s->memory[i], FLASH_SEGMENT_SIZE);
	}
	/* Now we should be bigger than the source */
	assert(d->byteSize >= s->byteSize);
}

void flashLoadCosts(char* fileName){
	FILE* f = fopen(fileName, "r"); 
	assert(f);
	char* format = "pagesize: %d, ebnumpages: %d, fixedwrite: %lf, fixedread: %lf, varwrite: %lf, varread: %lf, eraseblock: %lf, randwritefixed: %lf, randwritevar: %lf";
	int ret = fscanf(f, format, &NAND_PAGE_SIZE, &NAND_ERASE_SIZE, &FLASH_FIXED_WRITE_COST, &FLASH_FIXED_READ_COST, &FLASH_VARIABLE_WRITE_COST, &FLASH_VARIABLE_READ_COST, &FLASH_ERASE_COST, &FLASH_RANDOM_WRITE_FIXED_COST, &FLASH_RANDOM_WRITE_VAR_COST);
	if (ret > 7){
		assert(ret == 9);
		assert(FLASH_RANDOM_WRITE_VAR_COST > 0 || FLASH_RANDOM_WRITE_FIXED_COST > 0); /* Something has to be non zero */
	}else{
		assert(ret == 7);
		FLASH_RANDOM_WRITE_FIXED_COST = FLASH_FIXED_WRITE_COST;
		FLASH_RANDOM_WRITE_VAR_COST = FLASH_VARIABLE_WRITE_COST;
	}
	fprintf(stdout, format, NAND_PAGE_SIZE, NAND_ERASE_SIZE, FLASH_FIXED_WRITE_COST, FLASH_FIXED_READ_COST, FLASH_VARIABLE_WRITE_COST, FLASH_VARIABLE_READ_COST, FLASH_ERASE_COST);
	fclose(f); f = NULL;
}

int FlashDump(FILE* f){
	int i;
	fprintf(f, "#<numReads> <numBytesRead> <numWrites> <numBytesWritten> <numErased> <maxPagesWritten> <readCost> <writeCost> <eraseCost> <totalCost>\n");
	for(i=0;i < N_IDX; i++){
		fprintf(f, "%lld %lld %lld %lld %lld %lld %.1f %.1f %.1f %.1f\n", whichNumCost[i][OP_READ], whichByteCost[i][OP_READ], whichNumCost[i][OP_WRITE], whichByteCost[i][OP_WRITE], whichNumCost[i][OP_ERASE], numPagesWritten[i], whichCost[i][OP_READ], whichCost[i][OP_WRITE], whichCost[i][OP_ERASE], (whichCost[i][OP_READ]+whichCost[i][OP_WRITE]+whichCost[i][OP_ERASE]));
	}
	return STATUS_OK;
}


void flash_close(int partNumber){
/* This function should be indempotent to non existant partitions */
	free_up_flash_memory(partNumber);
}


/* Ensure that we always have atleast one erase block of memory ... incase the
 * guy decides to erase this block */

void check_for_growth(int partNum, off_t atleast){
	assert(partitions[partNum].actualPages < 0);
	if ((long long)(atleast + NAND_PAGE_SIZE* NAND_ERASE_SIZE) > partitions[partNum].byteSize){
		fprintf(stderr, "\nInfinite flash %d exhausted, current amount : %lld", partNum, partitions[partNum].byteSize);
		expand_flash_memory(partNum, SEGMENTS_TO_ALLOCATE_AT_TIME);
	}
}

void flash_open(int partitionNumber, int size){
	assert(size < 0 || size % NAND_ERASE_SIZE == 0);
	initialize_flash_memory(partitionNumber);
}

void register_partition(int partNum, int nPages_given){
	//Round the nPages to the nearest multiple of NAND_ERASE_SIZE
	bzero(&partitions[partNum], sizeof(PartitionInfo_t));
	int nPages = (nPages_given<0) ? -1 : NAND_ERASE_SIZE * ((int)((nPages_given + (NAND_ERASE_SIZE - 1))/NAND_ERASE_SIZE));
	assert(partNum >= 0 && partNum < MAX_FLASH_PARTITIONS);
	assert(nPages< 0 || ((nPages_given <= nPages) && (nPages % NAND_ERASE_SIZE == 0)));
	partitions[partNum].npages = nPages_given;
	partitions[partNum].actualPages = nPages;
	int size = partitions[partNum].actualPages < 0 ? -1 : (partitions[partNum].actualPages * NAND_PAGE_SIZE);
	flash_open(partNum, size);
}

void FlashZeroCounters(){
	int i, j;
	fprintf(stderr, "\nZeroing the flash counters");
	for(i=0;i<N_IDX; i++){
		for(j= 0 ;j< N_OPER; j++) {whichCost[i][j] = 0.0; whichNumCost[i][j] = 0; whichByteCost[i][j] = 0;}
		numPagesWritten[i] = 0;
	}
	for(i=0;i<MAX_FLASH_PARTITIONS;i++){
		partitions[i].num_writes = 0;
		partitions[i].num_reads = 0;
		partitions[i].num_erases = 0;
		partitions[i].readCost = 0;
		partitions[i].writeCost = 0;
		partitions[i].eraseCost = 0;
	}
}


int FlashInit()
{

	memset(partitions, 0, sizeof(partitions));
	
	// The freeBv, alt and alt second partitions will be set by the
	// respective nary tree init functions

	FlashZeroCounters();

	return STATUS_OK;
}

void accountIdxCost(int index, int oper, double cost, int ntimes, int nbytes){
	assert(index >= 0 && index < N_IDX);
	whichCost[index][oper] += cost;
	whichNumCost[index][oper] += ntimes;
	whichByteCost[index][oper] += nbytes;
}

int getFlashIndex(int callerId, int oper){
	assert(oper >=0 && oper < N_OPER);
	int index = -1;
	if (callerId == FLASH_LATREE_NODE){
		index = LATREE_NODE_INDEX;
	}else if (callerId == FLASH_MISC){
		index = -1;
	}else{
		fprintf(stderr, "\nInvalid flash caller type called %d", callerId);
		assert(false);
	}
	return index;
}

void accountFlashCost(int callerId, int oper, double cost, int hint, int partition, int ntimes, int nbytes){
	int index = getFlashIndex(callerId, oper);
	if (index < 0) return;
	accountIdxCost(index, oper, cost, ntimes, nbytes);
	if (partition >= 0){
		if (oper == OP_READ){
			partitions[partition].readCost+= cost;
			partitions[partition].num_reads += ntimes;
		}else if (oper == OP_WRITE){
			partitions[partition].writeCost += cost;
			partitions[partition].num_writes += ntimes;
		}else if (oper == OP_ERASE){
			partitions[partition].eraseCost += cost;
			partitions[partition].num_erases += ntimes;
		}else assert(false);
	}
}

double calculateFlashCost(int oper, int len, int* givenBytes, int* givenTimes)
{
	/* first incremement the totals*/
	double cost = 0;
	if (oper == OP_WRITE)
	{
		cost = FLASH_WRITE_COST(len);
	}
	else if (oper == OP_READ)
	{
		cost = FLASH_READ_COST(len);
	}
	else if (oper == OP_ERASE)
	{
		assert(len > 0);
		cost = len*FLASH_ERASE_COST;
	}else assert(false);
	assert(cost>0);
	assert(oper>=0 && oper < N_OPER);
	if (givenBytes){
		*givenBytes = (oper == OP_ERASE) ? len * NAND_ERASE_SIZE * NAND_PAGE_SIZE : len;
	}
	if (givenTimes) *givenTimes = (oper == OP_ERASE) ? len : 1;;
	return cost;
}

double traceDump(int partition, int callerId, int oper, offsetptr_t len, int hint)
{
	int nbytes, times;
	double cost = (callerId != FLASH_MISC) ? calculateFlashCost(oper, len, &nbytes, &times) : 0;
	if (cost > 0) accountFlashCost(callerId, oper, cost, hint, partition, times, nbytes);
	else assert (callerId == FLASH_MISC);
	return cost;
}


int FlashWrite(bool doRealIo, int partition, int callerId, pageptr_t page, offsetptr_t offset,
		void *data, offsetptr_t len, int hint)
{
	off_t desiredLoc = FLASH_OFFSET(page, offset);

	if (len > 0 && !data){
		data = zeroedDataBuf;
		assert(len < zeroedDataBufLen);
	}

	if (doRealIo){
		assert((partitions[partition].npages < 0 || page < partitions[partition].npages) && len <= NAND_PAGE_SIZE && offset <= NAND_PAGE_SIZE);
		if(partitions[partition].npages < 0) check_for_growth(partition, desiredLoc);
		do_flash_io(partition, FLASH_WRITE_DIR, desiredLoc, (unsigned char*)data, len);
	}
	traceDump(partition, callerId, OP_WRITE, len, hint);
	return (STATUS_OK);
}


int FlashRead(bool doRealIo, int partition, int callerId, pageptr_t page, offsetptr_t offset,
		void *data, offsetptr_t len, int hint) 
{

	off_t desiredLoc = FLASH_OFFSET(page, offset);
	if (len > 0 && !data){
		data = zeroedDataBuf;
		assert(len < zeroedDataBufLen);
	}
	if (doRealIo){
		assert((partitions[partition].npages < 0 || page < partitions[partition].npages) && len <= NAND_PAGE_SIZE && offset <= NAND_PAGE_SIZE);
		do_flash_io(partition, FLASH_READ_DIR, desiredLoc, (unsigned char*)data, len);
	}
	traceDump(partition, callerId, OP_READ, len, hint);
	return (STATUS_OK);
}

int FlashErase(bool doRealIo, int partition, int callerId, pageptr_t page, int hint){
	assert(page % NAND_ERASE_SIZE == 0);
	if (doRealIo){
		assert(partitions[partition].npages < 0 || page < partitions[partition].npages);
	}
	
	traceDump(partition, callerId, OP_ERASE, 1, hint);

	return STATUS_OK;
}

int FlashEraseRegion(bool doRealIo, int partition, int callerId, pageptr_t startPage, int hint, int numberOfPages){
	assert(numberOfPages % NAND_ERASE_SIZE == 0);
	pageptr_t endPage = startPage + numberOfPages;
	if (doRealIo) assert(startPage >= 0 && (partitions[partition].npages < 0 || endPage <= partitions[partition].npages));
	while(startPage < endPage){
		FlashErase(doRealIo, partition, callerId, startPage, hint);
		startPage += NAND_ERASE_SIZE;
	}
	return STATUS_OK;
}

int FlashDestroy(){
	int i;
	for (i = 0; i < MAX_FLASH_PARTITIONS; i++){
		flash_close(i);
	}
	return STATUS_OK;
}

void flashCostPageWritten(int callerId, int npages){
	int idx = getFlashIndex(callerId, OP_WRITE);
	if (idx < 0) return;
	numPagesWritten[idx] += 1;		
}

void copy_partition(int tovol, int fromvol){
	assert(tovol < MAX_FLASH_PARTITIONS && tovol >= 0);
	assert(fromvol < MAX_FLASH_PARTITIONS && fromvol >= 0);
    assert(partitions[tovol].npages < 0 || partitions[tovol].npages >= partitions[fromvol].npages);
	copy_flash_memory_partitions(tovol, fromvol);
}
