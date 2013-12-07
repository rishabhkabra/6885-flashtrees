#ifndef WRITE_METADATA_H
#define WRITE_METADATA_H
#include "util.h"
#include "types.h"

#define HDPARM_COMMAND "/sbin/hdparm -W 1 %s"
#define SETRA_COMMAND "/sbin/blockdev --setra 0 %s"

struct workDetails_t{
    ioOperation_t opType;
    address_t startByte;
    address_t size;
};

struct ioDetails_t{
  address_t lastReadPos;
  address_t lastWritePos;
  int numRandomReads;
  int numRandomWrites;
  int numTotalReads;
  int numTotalWrites;
  int numTotalOps;
  int BUF_SIZE;
  
ioDetails_t(int BUF_SIZE): BUF_SIZE(BUF_SIZE), numRandomWrites(0), numRandomReads(0), numTotalOps(0), numTotalWrites(0), numTotalReads(0), lastReadPos(-1), lastWritePos(-1) {assert(BUF_SIZE > 0);}
  
  void processOp(address_t addr, ioOperation_t op){//addr is the current block number being touched
    if (op == WRITE_OP){
      numTotalWrites ++; 
      if (lastWritePos >= 0){
	address_t diff = llabs(addr - lastWritePos);
	if (diff > 1) numRandomWrites ++;
      }
      lastWritePos = addr;
    }else{
      numTotalReads ++; 
      if (lastReadPos >= 0){
	address_t diff = llabs(addr - lastReadPos);
	if (diff > 1) numRandomReads ++;
      }
      lastReadPos = addr;
    }
    numTotalOps += 1;
  }
};

struct traceMetadata_t{
  address_t curpage;
  address_t size;
  address_t maxPageWritten;
  bool doLogging;
  int BUF_SIZE;
  int fd;
  unsigned char* writebuffer;
  unsigned char* readbuffer;
  bool directFlag;
  int bufSize;
  bool onlyTracing;
  double ioTimeSoFar;
  ioDetails_t ioCounters;
  
  ~traceMetadata_t(){
    if (fd >= 0){
      ::close(fd);
      fd = -1;
    }
  }
  
  void growTo(address_t byte){
    address_t pgNum = (byte + BUF_SIZE - 1)/BUF_SIZE; 
    if (pgNum <= 0) return;
    curpage = maxPageWritten = pgNum;
    assert(curpage >= 0 && curpage < size);
  }
  
traceMetadata_t(char* diskFile, int pageSize, bool directFlag, address_t firstWriteAddress, bool doLogging, bool onlyTracing) : BUF_SIZE(pageSize), fd(-1), curpage(0), writebuffer(NULL), bufSize(0), maxPageWritten(-1), directFlag(directFlag), readbuffer(NULL), doLogging(doLogging), onlyTracing(onlyTracing), ioTimeSoFar(0), ioCounters(pageSize) {
  address_t numblocks = 0;
  char cmdBuf[512];

  printf("direct flag: %d\n", directFlag);  

  snprintf(cmdBuf, sizeof(cmdBuf), SETRA_COMMAND, diskFile);
  fprintf(stderr, "\nRunning %s", cmdBuf);
  //system(cmdBuf);
  
  snprintf(cmdBuf, sizeof(cmdBuf), HDPARM_COMMAND, diskFile);
  fprintf(stderr, "\nRunning %s", cmdBuf);
  //system(cmdBuf);
  
  writebuffer = (unsigned char*)(getPageAlignedBuffer(16384, NULL));
  readbuffer = (unsigned char*)(getPageAlignedBuffer(16384, NULL));
  memset(writebuffer, 0xa1, 16384);

  fd = open(diskFile, O_RDWR);// | (directFlag ? O_DIRECT : 0));
  printf("opened %s with file descriptor %d\n", diskFile, fd);
  assert(fd >= 0);
  int retval = 0;
  numblocks = 712890496;//ioctl(fd, BLKGETSIZE, &numblocks);
  assert(retval >= 0);
  address_t maxDiskSize = (address_t)numblocks * 512;
  printf("setting this->size to %d\n", maxDiskSize/BUF_SIZE);
  this->size = maxDiskSize / BUF_SIZE ; /* NUM OF PAGES */
  if (firstWriteAddress > 0) growTo(firstWriteAddress - 1);
  if (onlyTracing){  /* Open and close it if we are not using it ... */
    ::close(fd);
    fd = -1;
  }
}
  
  void close(){
    flushBuffer();
  }
  
  void doRawWrite(address_t pageToWrite, int toWrite){
    assert(toWrite >= 0 && toWrite <= BUF_SIZE);
    //int bToWrite = directFlag ? BUF_SIZE : toWrite;
    int bToWrite = toWrite;
    printf("bToWrite: %d,  BUF_SIZE: %d, toWrite: %d\n", bToWrite, BUF_SIZE, toWrite);
    assert(pageToWrite >= 0 && pageToWrite < size);
    address_t seekPos = pageToWrite * BUF_SIZE;
    if (onlyTracing){
      assert(fd < 0);
      extern FILE* outputTraceFile;
      fprintf(outputTraceFile, "W %d %lld %lld\n", bToWrite, seekPos, pageToWrite);
    }else{
      address_t ret = lseek64(fd, seekPos ,SEEK_SET);
      assert(ret == seekPos);
      double t = nowTime();
      ret = write(fd, writebuffer, bToWrite);
      printf("ret = %d\n", ret);
      assert(ret == bToWrite);
      ioTimeSoFar += (nowTime() - t);
      ioCounters.processOp(pageToWrite, WRITE_OP);
    }
  }
  
  void doRawRead(address_t pageToRead, int sizeToRead){
    printf("raw read size: %d. buffer size: %d. size: %d\n", sizeToRead, BUF_SIZE, size);
    assert(sizeToRead >= 0 && sizeToRead <= BUF_SIZE);
    int bytesToRead = directFlag ? BUF_SIZE : sizeToRead;
    assert(pageToRead >= 0 && pageToRead < size);
    address_t seekPos = pageToRead * BUF_SIZE;
    if (onlyTracing){
      assert(fd < 0);
      extern FILE* outputTraceFile;
      fprintf(outputTraceFile, "R %d %lld %lld\n", bytesToRead, seekPos, pageToRead);
    } else{
      address_t ret = lseek64(fd, seekPos, SEEK_SET);
      assert(ret == seekPos);
      double t = nowTime();
      ret = read(fd, readbuffer, bytesToRead);
      printf("read return value: %d\n", ret);
      assert(ret == bytesToRead);
      ioTimeSoFar += (nowTime() - t);
      ioCounters.processOp(pageToRead, READ_OP);
    }
  }
  
  void flushBuffer(){
    if (bufSize <= 0) return;
    int offset = 0;
    int remaining = bufSize;
    do{
      assert(curpage >= 0 && curpage < size);
      int toCopy = BUF_SIZE;
      if (remaining < toCopy) toCopy = remaining;
      doRawWrite(curpage, toCopy);
      offset += toCopy;
      remaining -= toCopy;
      curpage++;
    }while(remaining > 0);
    if (curpage > maxPageWritten) maxPageWritten = curpage;
    bufSize = 0;
  }
  
  address_t currentSize(){
    return curpage * BUF_SIZE + bufSize;
  }
  
  void writeData(int len, address_t* address){
    int remaining = len;
    int offset = 0;
    if (len <= BUF_SIZE && (bufSize + len > BUF_SIZE)){
      flushBuffer();
    }
    do{
      int spaceLeft = BUF_SIZE - bufSize;
      assert(spaceLeft >= 0);
      if (spaceLeft == 0) flushBuffer();
      else{
	if (remaining < spaceLeft) spaceLeft = remaining;
	if (offset == 0 && address){
	  *address = currentSize();
	}
	offset+= spaceLeft;
	remaining -= spaceLeft;
	bufSize += spaceLeft;
      }
    }while(remaining > 0);
  }
  
  void readData(address_t address, int len){
    address_t curMaxAddress = currentSize();
    assert(address >= 0 && address < curMaxAddress);
    assert(address + len <= curMaxAddress);
    int remaining = len;
    int offset = 0;
    do{
      address_t pageToRead = address / BUF_SIZE;
      assert(pageToRead <= curpage);
      int startOffset = address % BUF_SIZE;
      int toCopy = BUF_SIZE - startOffset;
      if (toCopy > remaining) toCopy = remaining;
      assert(toCopy <= BUF_SIZE);
      if (pageToRead < curpage){
	doRawRead(pageToRead, toCopy);
      }else{
	assert(startOffset + toCopy <= bufSize);
      }
      offset+= toCopy;
      remaining -= toCopy;
      assert(remaining >= 0);
      address += toCopy;
    }while(remaining > 0);
  }
  
  void doRawIo(workDetails_t& d){
    address_t pg = d.startByte/BUF_SIZE;
    if (d.opType == WRITE_OP){
      doRawWrite(pg, d.size);
    }else doRawRead(pg, d.size);
  }

  void takeTrace(workDetails_t& d){
    if (doLogging){
      if (d.opType == WRITE_OP){
	address_t wentTo;
	writeData(d.size, &wentTo);
      }else if (d.opType == READ_OP){
	readData(d.startByte, d.size);
      }
    }else{
      doRawIo(d);
    }
  }
};
#endif
