#ifndef WRITE_METADATA_H
#define WRITE_METADATA_H

#include "flash.h"
#include <string>
#include "buf_address.h"
#include <cstdlib>
#include <cstring>

using namespace std;
extern bool actualIndexOpsStarted;
extern bool doWeWriteAtSamePlace;
extern bool doIoLogging;
struct writeMetadata_t{
	address_t curpage;
	address_t size;
	int volume;
	address_t maxPageWritten;
	unsigned char* buffer;
	int bufSize;
	int from;
    FILE* logFile;
	bool amIAux;
	bool doRealIo;
	const int BUF_SIZE;

	~writeMetadata_t(){
		if (buffer){
			free(buffer);
			buffer = NULL;
		}
        if (logFile){
            fclose(logFile);
            logFile = NULL;
        } 
        flash_close(volume);
	}

	writeMetadata_t(address_t size, int volume, int account, bool doLogging, bool amIAux, bool doRealIo) : curpage(0), size(size), volume(volume), maxPageWritten(-1), buffer(NULL), bufSize(0), from(account), logFile(NULL), amIAux(amIAux), doRealIo(doRealIo), BUF_SIZE(NAND_PAGE_SIZE){
        extern char* OUTPUT_FILE_NAME;
		assert(BUF_SIZE > 0);
		buffer = (unsigned char*)malloc(BUF_SIZE);
		assert(buffer);
		assert(volume >= 0);
		register_partition(volume, this->size);
        logFile = NULL;
        if (OUTPUT_FILE_NAME && doLogging){
           char fileName[1024];
           snprintf(fileName, sizeof(fileName), "%s_volumetrace_%d_%d", OUTPUT_FILE_NAME, volume, account);
           logFile = fopen( fileName, "w" );
           assert(logFile);
        }
	}

    void retract(){
		curpage = 0;
		maxPageWritten = -1;
		bufSize = 0;
    }

    void changeAccount(int newAccount){
        from = newAccount;
    }

	void close(){
		flushBuffer();
	}

	void doRawWrite(address_t pg, int offset, void* data, int len){
		int ret = FlashWrite(doRealIo, volume, from, (pageptr_t)pg, offset, data, len, 0);
		assert(ret == STATUS_OK);
        if (logFile && actualIndexOpsStarted && doIoLogging){
			int npg = ROUND_UP_TO_PAGE_SIZE(len);
			assert(npg > 0);
			fprintf(logFile, "W %d %lld %d\n", len, pg * NAND_PAGE_SIZE + offset, npg);
		}
	}

	void flushBuffer(){
		assert(size < 0 || curpage < size);
		if (bufSize <= 0) return;
		int offset = 0;
		int remaining = bufSize;
		do{
			int toCopy = NAND_PAGE_SIZE;
			if (remaining < toCopy) toCopy = remaining;
			doRawWrite(curpage, 0, buffer + offset, toCopy);
			flashCostPageWritten(from);
			offset += toCopy;
			remaining -= toCopy;
			curpage++;
		}while(remaining > 0);
		bufSize = 0;
		if (curpage > maxPageWritten) maxPageWritten = curpage;
	}

    address_t pagesWritten(){
        return curpage + (bufSize > 0 ? 1 : 0);
    }

	address_t writePagesLeft(){
		assert(size > 0);
		int left = (size - pagesWritten());
		if (left < 0) left = 0;
		return left;
	}
	
	bool willFillSpaceUpFillUp(int extra){
		if (size < 0) return false;
		else return (extra > writePagesLeft());
	}

	address_t currentSize(){
		return curpage * NAND_PAGE_SIZE + bufSize;
	}

	bool avoidFtl(){
		/* TODO */
		return doWeWriteAtSamePlace || amIAux || !actualIndexOpsStarted;
	}

	void writeData(unsigned char* buf, int len, address_t* address){
		int remaining = len;
		int offset = 0;
		if (!buf){
			buf = zeroedDataBuf;
			assert(len <= zeroedDataBufLen);
		}
		assert(buf);

		if (avoidFtl() && address && is_valid(*address)){
            assert(*address >= 0);
			assert((*address) < currentSize());
			address_t pageAddr = (*address)/NAND_PAGE_SIZE;
			doRawWrite(pageAddr, (*address) % NAND_PAGE_SIZE, buf, len);
		}else{
			if (len <= BUF_SIZE && (bufSize + len > BUF_SIZE)) flushBuffer();
			do{
				int spaceLeft = BUF_SIZE - bufSize;
				assert(spaceLeft >= 0);
				if (spaceLeft == 0) flushBuffer();
				else{
					if (remaining < spaceLeft) spaceLeft = remaining;
					memcpy(buffer+ bufSize, buf + offset, spaceLeft);
					if (offset == 0 && address){
						*address = currentSize();
					}
					offset+= spaceLeft;
					remaining -= spaceLeft;
					bufSize += spaceLeft;
				}
			}while(remaining > 0);
		}
	}

	void doRawRead(address_t pg, int offset, void* data, int len, bool cheatMode){
		int ret = FlashRead(doRealIo, volume, cheatMode ? FLASH_MISC : from, (pageptr_t)pg, offset, data, len, 0);
		assert(ret == STATUS_OK);
        if (!cheatMode && logFile && actualIndexOpsStarted && doIoLogging){
			int npg = ROUND_UP_TO_PAGE_SIZE(len);
			assert(npg > 0);
			fprintf(logFile, "R %d %lld %d\n", len, pg * NAND_PAGE_SIZE + offset, npg);
		}
	}

	void readData(unsigned char* buf, address_t address, int len, bool cheatMode = false){
		address_t curMaxAddress = currentSize();
		assert(address >= 0 && address < curMaxAddress);
		assert(address + len <= curMaxAddress);
		if (!buf){
			buf = zeroedDataBuf;
			assert(len <= zeroedDataBufLen); 
		}
		assert(buf);
		int remaining = len;
		int offset = 0;
		do{
			address_t pageToRead = address / NAND_PAGE_SIZE;
			assert(pageToRead <= curpage);
			int startOffset = address % NAND_PAGE_SIZE;
			int toCopy = NAND_PAGE_SIZE - startOffset;
			if (toCopy > remaining) toCopy = remaining;
			assert(toCopy <= NAND_PAGE_SIZE);
			if (pageToRead < curpage){
				doRawRead(pageToRead, startOffset, buf + offset, toCopy, cheatMode);
			}else{
				assert(startOffset + toCopy <= bufSize);
				memcpy(buf + offset, buffer + startOffset, toCopy);
			}
			offset+= toCopy;
			remaining -= toCopy;
			assert(remaining >= 0);
			address += toCopy;
		}while(remaining > 0);
	}

    void eraseSelf(bool cheatMode){
        if (curpage > 0){
            FlashEraseRegion(doRealIo, volume, cheatMode ? FLASH_MISC : from, 0, 0, ROUND_UP_TO_ERASE_BLOCK_NPAGES(curpage));
        }
        curpage = 0;
	    maxPageWritten = -1;
        bufSize = 0;
    }

	double calculateFlashCost(int oper, int size){
		if (oper == OP_WRITE){
			return ((double)size/NAND_PAGE_SIZE) * PAGE_WRITE_COST;
		}else if (oper == OP_READ){
			int remaining = size;
			double cost = 0;
			while(remaining > 0){
				int thisTime = (remaining < NAND_PAGE_SIZE) ? remaining : NAND_PAGE_SIZE;
				cost += ::calculateFlashCost(oper, thisTime, NULL, NULL);
				remaining -= thisTime;
			}
			return cost;
		}else return ::calculateFlashCost(oper, size, NULL, NULL); //for erase
	}

    void copyIn(writeMetadata_t* m){
        bufSize = m->bufSize; 
        from = m->from;
        curpage = m->curpage;
        maxPageWritten = m->maxPageWritten;
        memcpy(buffer, m->buffer, BUF_SIZE);
		assert(m->volume >= 0);
		copy_partition(volume, m->volume);
    }
	
	void ensureAddress(address_t a){
		assert(size < 0 || a < size);
		address_t nextPage = (a/NAND_PAGE_SIZE) + 1;
		/* Ensure that curpage is always atleast one page after this guy */
		if (curpage < nextPage) curpage = nextPage;
		if (maxPageWritten < 0 || curpage > maxPageWritten) maxPageWritten = curpage;
	}
};

#endif
