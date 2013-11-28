#include <buffer_interface.h>
#include <tree_structures.h>
#include <flash.h>
#include <map>
#include <set>
#include <vector>
#include <math.h>
#include <list>
#include <algorithm>
#include "bmgr.h"
#include "my_debug.h"
#include "subtree_tree.h"

/* Buffers are uniquely identified by their startPages here ... which is unique .. as we map it by the bufferNumber */
	
using namespace std;

extern bool forceFlushFlush;
extern int FLUSHING_DELTA_POINT;
extern int COMPARE_KEY(index_key_t k1, index_key_t k2);
int _flushBufferThreshhold = 3;
int numHangingSamples = 0;
int numberOfHangingDeltasSum = 0;
struct internalBuffer_t;

typedef dense_hash_map<int, internalBuffer_t*> internalBufferMap_t;

static internalBufferMap_t* internalBufferMap = NULL;
int NUM_BUF_PAGES_WRITTEN_TO_FLASH = 0;
int NUM_BUF_DELTAS_WRITTEN_TO_FLASH = 0;
int MAX_HANGING_DELTAS_ALLOWED = 0;
int REALLY_MAX_HANGING_DELTAS_ALLOWED = 0;
int currentlyStolenAmount = 0;
int numberOfHangingDeltas = 0;
int maxHangingAround = 0;

#define MAX_MAX_DELTAS_ALLOWED ((8<<20)/ (int)(sizeof(tree_delta_t)) )

writeMetadata_t* bufferMetadata = NULL;

struct segmentDetail_t{
	int numberOfEntries;
	address_t startByte;
	segmentDetail_t(address_t startByte, int numberOfEntries): numberOfEntries(numberOfEntries), startByte(startByte) { assert(startByte >= 0);}
	segmentDetail_t(const segmentDetail_t& detail): numberOfEntries(detail.numberOfEntries), startByte(detail.startByte) {}
};

static hangingBuffers_t* hangingAround = NULL;
int getAndRemoveHangingBuffer(int startPage);
void getHangingAround(int startPage); /* It tries to stick this bunch of deltas in or flushes it immediately */
void ensureNoHanging();

buf_manager_t* nodeBufMgr = NULL;
vector<int> memoryBuffersTaken;

int deltasToBytes(int deltas){
    assert(nodeBufMgr);
    return (int)(( ((float)deltas) / (nodeBufMgr->dataSize/sizeof(tree_delta_t)) ) * nodeBufMgr->dataSize);
}


int bytesToDeltas(int bytes){
    assert(nodeBufMgr);
    return (int)( (((float)bytes)/nodeBufMgr->dataSize) * (nodeBufMgr->dataSize/sizeof(tree_delta_t)));
}

int askOneMemBuffer(){
    assert(nodeBufMgr);
    node_buf_t* b = buf_manager_new_buf(nodeBufMgr, -1, true);
    if (!b) return STATUS_FAILED;
    b->noCallback = true;
    buf_pin(nodeBufMgr, b);
    /* fprintf(stderr, "\nAsked for one more node buffer -- %d", b->bufNumber); */
    memoryBuffersTaken.push_back(b->bufNumber);
    return STATUS_OK;
}

void returnOneMemBuffer(){
    assert(nodeBufMgr);
    assert((int)(memoryBuffersTaken.size()) >= 1);
    int bNumber = memoryBuffersTaken.back();
    memoryBuffersTaken.pop_back();
    /* fprintf(stderr, "\nReleased one more node buffer -- %d", bNumber); */
    buf_immediately_release(nodeBufMgr, bNumber);
}

void checkForBufferMemShrinkage(){
    assert(nodeBufMgr);
    if (MAX_HANGING_DELTAS_ALLOWED < 0) return;
    while(1){
        int memoryAllocated = (int)(memoryBuffersTaken.size()) * nodeBufMgr->dataSize;
        int memoryCurrentlyUsed = deltasToBytes(numberOfHangingDeltas + currentlyStolenAmount);
        if (memoryAllocated - memoryCurrentlyUsed >= nodeBufMgr->dataSize){
            returnOneMemBuffer();
			assert(MAX_HANGING_DELTAS_ALLOWED > 0);
            assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
            MAX_HANGING_DELTAS_ALLOWED -= bytesToDeltas(nodeBufMgr->dataSize);
            assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
            assert(MAX_HANGING_DELTAS_ALLOWED >= 0);
        }else break;
    }
    assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
    assert(MAX_HANGING_DELTAS_ALLOWED + currentlyStolenAmount <= REALLY_MAX_HANGING_DELTAS_ALLOWED);
}

void checkForBufferMemGrowth(int deltasRequired){
    if (MAX_HANGING_DELTAS_ALLOWED < 0 || deltasRequired + numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED) return;
    assert(nodeBufMgr);
    int room = REALLY_MAX_HANGING_DELTAS_ALLOWED - (MAX_HANGING_DELTAS_ALLOWED + currentlyStolenAmount);
    if (room <= 0) return;
    if (room > deltasRequired){
        room = deltasRequired;
    }
    /* We will ask for this much memory */
    int memoryToAsk = deltasToBytes(room);
    int memoryAllocated = ((int)(memoryBuffersTaken.size())) * nodeBufMgr->dataSize;
    int memoryCurrentlyUsed = deltasToBytes(numberOfHangingDeltas + currentlyStolenAmount);
    if (memoryToAsk + memoryCurrentlyUsed > memoryAllocated){
        /* Need to ask for more allocation */
        int buffersNeeded = ROUND_UP_NUM_BUFFERS((memoryToAsk + memoryCurrentlyUsed - memoryAllocated), nodeBufMgr->dataSize);
        while(buffersNeeded-- > 0){ 
            int ret = askOneMemBuffer();
            if (ret == STATUS_FAILED){
                fprintf(stdout, "\nCouldn't get one more buffer for the buffer cache -- although we are underlimit");
                break; /* Couldn't grow further !*/
            }
            MAX_HANGING_DELTAS_ALLOWED += bytesToDeltas(nodeBufMgr->dataSize);
        }
    }
    assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
    assert(MAX_HANGING_DELTAS_ALLOWED + currentlyStolenAmount <= REALLY_MAX_HANGING_DELTAS_ALLOWED);
}

struct list_buffer_t{
	int bufferNumber;
	int startPage;
	void print(){
		fprintf(stderr, "\nBuffer for buffer %d, starting at %d", bufferNumber, startPage);
	}
};

list_buffer_t* _get_list_buffer(tree_buffer_t* buf);

void buffer_info_init(bufferInfo_t* info){
	assert(info);
	info->currentPage = 0;	
	info->numberEntries = 0;
	info->numberInsertEntries = 0;
	info->isBufferFull = false;
	info->minQueryTimeStamp = -1;
	info->prematureFull = false;
    info->maxKey = INVALID_KEY;
    info->minKey = INVALID_KEY;
}

double deltaPageWriteCost(int nEntries, bool doAccount){
	int nBytes, nTimes;
    int headerSize = sizeof(bufferSegmentHeader_t);
	double cost = calculateFlashCost(OP_WRITE, nEntries*sizeof(tree_delta_t) + headerSize, &nBytes, &nTimes);
	return cost;
}

struct list_buffer_iterator_t;

struct internalBuffer_t{
	bufferInfo_t* info;
	vector<tree_delta_t> deltaList;
	vector<segmentDetail_t> segmentDetails;
	int deltasInCurPage;
	int bufferNumber;
	int level;
	
	void flushBuffer(){
		if (deltasInCurPage > 0){
			//write page
			assert(deltasInCurPage <= DELTAS_IN_PAGE);
			info->currentPage ++;
			NUM_BUF_DELTAS_WRITTEN_TO_FLASH += deltasInCurPage;
			NUM_BUF_PAGES_WRITTEN_TO_FLASH ++;
			address_t wentTo = 0;
			wentTo = INVALID_ADDRESS;
			bufferMetadata->writeData(NULL, deltasInCurPage*sizeof(tree_delta_t), &wentTo);
			segmentDetail_t newSegment(wentTo, deltasInCurPage);
			segmentDetails.push_back(newSegment);
			assert((int)(segmentDetails.size()) == info->currentPage);
			deltasInCurPage = 0;
		}
	}

	void init(){
		deltaList.clear();
		buffer_info_init(info);
		segmentDetails.clear();
		deltasInCurPage = 0;
	}

	internalBuffer_t(bufferInfo_t* info, int bufferNumber, int level) : info(info), bufferNumber(bufferNumber), level(level) {
		init();
	}

	void print(){
		tree_buffer_info_print(info);
		fprintf(stderr, "\nList based internalBufferMap buffer: (deltasInCurPage: %d, currentPageSize: %d), print deltas \n", deltasInCurPage, info->currentPage);
		assert( (int)(deltaList.size()) == info->numberEntries);
		for(vector<tree_delta_t>::iterator i = deltaList.begin(); i != deltaList.end(); i++){
			print_tree_delta(&(*i));
		}
		fprintf(stderr, "\n");
	}

	double accountForPartialPageRead(int nEntries, int* nbytes, int* ntimes){
		assert(nEntries <= DELTAS_IN_PAGE);
		int nb = 0, nt = 0;
		int gb, gt;
		double cost = 0; 
		int headerSizeToRead = sizeof(bufferSegmentHeader_t);
		cost += calculateFlashCost(OP_READ, nEntries*sizeof(tree_delta_t) + headerSizeToRead, &gb, &gt);
		nb+= gb; nt += gt;
		if (nbytes) *nbytes = nb;
		if (ntimes) *ntimes = nt;
		return cost;
	}

	double accountForWritingWhole(){
		int nsize = deltaList.size();
		if (nsize == 0) return 0;
		double deltasPerPage = (NUM_BUF_DELTAS_WRITTEN_TO_FLASH == 0 || NUM_BUF_PAGES_WRITTEN_TO_FLASH == 0) ? DELTAS_IN_PAGE : ((double)NUM_BUF_DELTAS_WRITTEN_TO_FLASH)/NUM_BUF_PAGES_WRITTEN_TO_FLASH ;
		assert(deltasPerPage <= DELTAS_IN_PAGE);
		int pagesToWrite = (int)(nsize/deltasPerPage);
		double cost = 0;
		int dPerPage = (int)deltasPerPage;
		for(int i = 0; i < pagesToWrite; i++){
			cost += deltaPageWriteCost(dPerPage, false);
		}
		return cost;
	}

    int findKeysInRange(int startPage, range_t rangeToFind, double* costWithoutCaching, double* realCost, vector<flash_bt_data_t>& keys, subtreeDetails_t* details);

#if 0 /* Can  do partial buffer scans, we don't use it now */
	int findKey(index_key_t key, location_t* loc, double* costWithoutCaching, double* realCost, subtreeDetails_t* details){
		int ret = STATUS_FAILED;
		assert((int)(segmentDetails.size()) == info->currentPage);
		assert(info->numberEntries == (int)(deltaList.size()) );
		int entriesRead = 0;
		while(ret != STATUS_OK && entriesRead < (int)(deltaList.size())){
			tree_delta_t delta = deltaList.at(entriesRead);
			if (delta.key == key){
				if (loc) *loc = delta.loc;
				ret= STATUS_OK;
			}
			entriesRead++;
		}
		accountBufferReadCost(entriesRead, costWithoutCaching, realCost, true, details);
		return ret;
	}
#endif

	int accountBufferReadCost(int entriesToRead, double* costWithoutCaching, double* realCost, bool doAccount, subtreeDetails_t* details){
		/* Random correctness check: Are we still the same buffer that we were loaded into ? */
		if (details){
			assert(details->subtreeLevel == level);
			assert(details->bufferNumber == bufferNumber);
		}
		int ret = STATUS_FAILED;
		int entriesRead = 0;
		int entriesReadInThisPage = 0;
		assert((int)(segmentDetails.size()) == info->currentPage);
		assert(info->numberEntries == (int)(deltaList.size()));
		int entriesReadForFree = 0;
		double cost = 0;
		if (entriesToRead < 0){ //Did the caller ask us to read the entire buffer ?
			entriesToRead = (int)(deltaList.size());
		}
		int nb = 0, nt = 0;
		int gb, gt;
		int numPagesReallyRead = 0;
		int numPagesCachedRead = 0;
		vector<segmentDetail_t> segsRead;
		vector<segmentDetail_t>::iterator it = segmentDetails.begin();
		int entriesAccounted = 0;
		while(entriesRead < entriesToRead){
			if (entriesRead >= (int)(deltaList.size()) - deltasInCurPage){
				/* Reading from the cached 'buffer' page .. which hasn't been flushed to the flash yet */
				assert(entriesReadInThisPage == 0);
				assert(deltasInCurPage > 0);
				entriesReadForFree ++;
			}else{
				if (entriesReadInThisPage == 0){
					/* Going to read a new segment ... account for its cost */
					assert(it != segmentDetails.end());
					assert(it->numberOfEntries > 0);
					assert(it->startByte <= bufferMetadata->currentSize());
					assert(it->numberOfEntries <= DELTAS_IN_PAGE);
					int bytesRead = it->numberOfEntries * sizeof(tree_delta_t);
					entriesAccounted += it->numberOfEntries;
					if (doAccount){
						bufferMetadata->readData(NULL, it->startByte, bytesRead);
						segsRead.push_back(segmentDetail_t(it->startByte, bytesRead));
					}
					cost += accountForPartialPageRead(it->numberOfEntries, &gb, &gt);
					nb += gb; nt += gt;
					numPagesReallyRead ++;
				}
				entriesReadInThisPage++;
				if (entriesReadInThisPage == it->numberOfEntries){
					entriesReadInThisPage = 0;
					it++;
				}
			}
			entriesRead++;
		}

		assert(entriesRead < deltaList.size() || entriesAccounted == entriesRead - entriesReadForFree);
		if (realCost) *realCost = cost;
		if (entriesReadForFree > 0){
			cost += accountForPartialPageRead(entriesReadForFree, NULL, NULL);
			numPagesCachedRead ++;
		}
		if (costWithoutCaching) *costWithoutCaching = cost;
		return ret;
	}
}; /* End of internalBuffer_t struct */

void changeNumberOfHangingAroundDeltas(int newValue){
	numHangingSamples++;
	assert(newValue >= 0);
	assert(MAX_HANGING_DELTAS_ALLOWED < 0 || newValue <= MAX_HANGING_DELTAS_ALLOWED);
	numberOfHangingDeltasSum += newValue;
	numberOfHangingDeltas = newValue;
}

void ensureNoHanging(){
	assert(numberOfHangingDeltas == 0);
	assert(hangingAround->size() == 0);
}

int getAndRemoveHangingBuffer(int startPage){
	int ret = 0;
	hangingBuffers_t::iterator hangingIter = hangingAround->find(startPage);
	if(hangingIter != hangingAround->end()){
		ret = hangingIter->second;
		assert(ret > 0);
		hangingAround->erase(hangingIter);
		changeNumberOfHangingAroundDeltas(numberOfHangingDeltas - ret);
	}
	assert(hangingAround->find(startPage) == hangingAround->end());
	return ret;
}

void evictHangingDelta(int deltasToFree, int startPageNotToFree){
	int nFreed = 0;
    int cantFree = 0;
    int hangingOfNotToFree = 0;
    int rootBufferStartPage = -1;
    extern void getTheRootBuffer(tree_buffer_t*);
    tree_buffer_t rootbuf;
    getTheRootBuffer(&rootbuf);
	list_buffer_t* listBuf = _get_list_buffer(&rootbuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
    rootBufferStartPage = listBuf->startPage; /* Remember the root buffer start page, so that we don't evict it */
	if (startPageNotToFree >= 0){  
        assert(internalBufferMap->find(startPageNotToFree) != internalBufferMap->end());
		internalBuffer_t* t = (*internalBufferMap)[startPageNotToFree];
        if (hangingAround->find(startPageNotToFree) != hangingAround->end()){
			hangingOfNotToFree = (*hangingAround)[startPageNotToFree];
            assert(t->deltasInCurPage == hangingOfNotToFree);
            cantFree = hangingOfNotToFree;
        }
        if (deltasToFree > numberOfHangingDeltas - cantFree){
            /* We have to evict the startPageNotToFree's buffer also --- else we can't meet the required deltasToFree */
            startPageNotToFree = -1;
        }
    }
	assert(deltasToFree > 0);
	while(nFreed < deltasToFree){
		/* Evict the guy having the maximum number of deltas .. to ammortize the fixed cost */
		assert(hangingAround->size() > 0);
		/* Find a candidate for eviction */
		int startPageOfBufferToEvict = -1;
		int maxDeltasFound = -1;
		hangingBuffers_t::iterator hangingIter = hangingAround->begin();
		assert(hangingIter != hangingAround->end());
		while(hangingIter != hangingAround->end()){
			int startPage = hangingIter->first;				
			assert(internalBufferMap->find(startPage) != internalBufferMap->end());
			internalBuffer_t* t = (*internalBufferMap)[startPage];
			assert(t->deltasInCurPage == hangingIter->second);
			assert(hangingIter->second > 0);
			if ( (startPageNotToFree < 0 || startPage != startPageNotToFree) && startPage != rootBufferStartPage && hangingIter->second > maxDeltasFound){
				startPageOfBufferToEvict = startPage;
				maxDeltasFound = hangingIter->second;
			}
			hangingIter ++;
		}
		assert(maxDeltasFound > 0);
		assert(startPageOfBufferToEvict >= 0);
		internalBuffer_t* t = (*internalBufferMap)[startPageOfBufferToEvict];
		assert(t->deltasInCurPage == maxDeltasFound);
		t->flushBuffer();
		nFreed += maxDeltasFound;
		int ret = getAndRemoveHangingBuffer(startPageOfBufferToEvict);
		assert(ret == maxDeltasFound);
	}
}

void getHangingAround(int startPage){
	hangingBuffers_t::iterator hangingIter = hangingAround->find(startPage);
	assert(hangingIter == hangingAround->end());
	assert(internalBufferMap->find(startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[startPage];
	assert(t->deltasInCurPage > 0 && t->deltasInCurPage <= DELTAS_IN_PAGE);
    checkForBufferMemGrowth(t->deltasInCurPage);
	if ( MAX_HANGING_DELTAS_ALLOWED >= 0 && (MAX_HANGING_DELTAS_ALLOWED == 0 || t->deltasInCurPage > MAX_HANGING_DELTAS_ALLOWED)){
		/* Queing up deltas not allowed .. flush immediately */
		t->flushBuffer();	
	}else{
		assert(MAX_HANGING_DELTAS_ALLOWED > 0 || MAX_HANGING_DELTAS_ALLOWED < 0);
		if (MAX_HANGING_DELTAS_ALLOWED > 0 && numberOfHangingDeltas + t->deltasInCurPage > MAX_HANGING_DELTAS_ALLOWED){
			int deltasToFree = t->deltasInCurPage  + numberOfHangingDeltas - MAX_HANGING_DELTAS_ALLOWED;
			evictHangingDelta(deltasToFree, startPage);
		}
		assert(MAX_HANGING_DELTAS_ALLOWED < 0 || numberOfHangingDeltas + t->deltasInCurPage <= MAX_HANGING_DELTAS_ALLOWED);
		hangingAround->insert(make_pair(startPage, t->deltasInCurPage));					
		changeNumberOfHangingAroundDeltas(numberOfHangingDeltas + t->deltasInCurPage);
		if (numberOfHangingDeltas >= maxHangingAround + 100){
			maxHangingAround = numberOfHangingDeltas;
		}
	}
}


int tree_buffer_init(writeMetadata_t* m){
    assert(nodeBufMgr);
    if (REALLY_MAX_HANGING_DELTAS_ALLOWED >= 0){
        assert(REALLY_MAX_HANGING_DELTAS_ALLOWED > 0);
        REALLY_MAX_HANGING_DELTAS_ALLOWED = bytesToDeltas( ROUND_UP_NUM_BUFFERS(deltasToBytes(REALLY_MAX_HANGING_DELTAS_ALLOWED), nodeBufMgr->dataSize) *  nodeBufMgr->dataSize);
    }
	internalBufferMap = new internalBufferMap_t;
	assert(internalBufferMap);
	internalBufferMap->set_empty_key(INVALID_NODE);
	internalBufferMap->set_deleted_key(BAD_NODE);
	hangingAround = new hangingBuffers_t;
	assert(hangingAround);
	hangingAround->set_empty_key(INVALID_NODE);
	hangingAround->set_deleted_key(BAD_NODE);
	register_partition(BUFFER_VOLUME, -1);
	assert(!bufferMetadata);
	bufferMetadata = m;
	assert(bufferMetadata);
	return STATUS_OK;
}

int tree_buffer_destroy(){
	for(internalBufferMap_t::iterator i = internalBufferMap->begin(); i != internalBufferMap->end(); i++){
		assert(i->second);
		delete i->second;
	}
    flash_close(BUFFER_VOLUME);
	delete internalBufferMap;
	delete hangingAround; /* Assumed that the hanging around buffers have been flushed */
	return STATUS_OK;
}

tree_buffer_t tree_buffer_alloc(){
	list_buffer_t* buffer = new list_buffer_t;
	assert(buffer);
	memset(buffer, 0, sizeof(list_buffer_t));
	return (tree_buffer_t)(buffer);
}

list_buffer_t* _get_list_buffer(tree_buffer_t* buf){
	assert(buf && *buf);
	list_buffer_t* listBuf = (list_buffer_t*)(*buf);
	return listBuf;
}

int tree_buffer_node_destroy(tree_buffer_t* buf){
	if (buf && *buf){
		list_buffer_t* listBuf = _get_list_buffer(buf);
		DEBUG_FILE_PRINTF(stderr, "\nDestroying the list buffer for buffer %d", listBuf->bufferNumber);
		delete listBuf;
		*buf = NULL;
	}
	return STATUS_OK;
}

/* The buffer is not loaded into memory or such .. As for adding we don't  need
 * the buffer to be in memory .. only its metadata */

int tree_buffer_load(tree_buffer_t* buf, subtreeDetails_t* details, int startPage, bufferInfo_t* bufInfo){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(details);
	int bufferNumber = details->bufferNumber;
	int level = details->subtreeLevel;
	DEBUG_FILE_PRINTF(stderr, "\nCreating the buffer for buffer number %d", bufferNumber);
	internalBuffer_t* t = NULL;
	assert(startPage >= 0);
	if (internalBufferMap->find(startPage) != internalBufferMap->end()){
		t = (*internalBufferMap)[startPage];
		assert(t->bufferNumber == bufferNumber);
	}else{
		t = new internalBuffer_t(bufInfo, bufferNumber, level);
		(*internalBufferMap)[startPage] = t;
	}
	assert(t);
	listBuf->bufferNumber = bufferNumber;
	listBuf->startPage = startPage;
	return STATUS_OK;
}

int list_buffer_number_query_deltas(internalBuffer_t* t){
	bufferInfo_t* info = t->info;
	return (info->numberEntries > 0 ? (info->numberEntries - info->numberInsertEntries) : 0);	
}

void _check_for_full(internalBuffer_t* t){
	t->info->isBufferFull = ((int)(t->deltaList.size()) >= t->info->maxEntries);
}

int tree_buffer_start_adding(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	int nDeltasHanging = getAndRemoveHangingBuffer(listBuf->startPage);
	assert(t->deltasInCurPage == nDeltasHanging);
    checkForBufferMemShrinkage();
	return STATUS_OK;
}

int tree_buffer_add_delta(tree_buffer_t* buf, tree_delta_t* delta, node_t emptiedFrom){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	t->deltaList.push_back(*delta);
	assert(t->deltasInCurPage < DELTAS_IN_PAGE && t->deltasInCurPage >= 0);
	t->deltasInCurPage ++;
    if ((t->info->maxKey == INVALID_KEY) || COMPARE_KEY(t->info->maxKey, delta->key) < 0){
         t->info->maxKey = delta->key;
    }
    if ((t->info->minKey == INVALID_KEY) || COMPARE_KEY(t->info->minKey, delta->key) > 0){
         t->info->minKey = delta->key;
    }
	t->info->numberInsertEntries ++;
	t->info->numberEntries++;
	if (t->deltasInCurPage == DELTAS_IN_PAGE){
		t->flushBuffer();
	}
	_check_for_full(t);
	return STATUS_OK;
}

int tree_buffer_end_adding(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	if (t->deltasInCurPage > 0){//still has deltas to be flushed
		assert(t->deltasInCurPage <= DELTAS_IN_PAGE);
		getHangingAround(listBuf->startPage);
	}
	_check_for_full(t);
	return STATUS_OK;
}

int tree_buffer_emptied(tree_buffer_t* buf, node_t emptiedFrom, subtreeDetails_t* details){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	getAndRemoveHangingBuffer(listBuf->startPage);
    checkForBufferMemShrinkage();
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	if (details){
		assert(t->bufferNumber == details->bufferNumber);
		assert(t->level == details->subtreeLevel);
	}
	t->init();
	return STATUS_OK;
}

bool tree_buffer_is_full(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	return t->info->isBufferFull;
}

int tree_buffer_print(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	listBuf->print();
	t->print();
	return STATUS_OK;
}

int tree_buffer_info_print(bufferInfo_t* bufferInfo){
	assert(bufferInfo);
	fprintf(stderr, "\nBuffer Info: nEntries: %d, nInsertEntries: %d, currentPage: %d, isBufferFull: %s, prematureFull: %s, minQueryTimeStamp: %d", bufferInfo->numberEntries, bufferInfo->numberInsertEntries, bufferInfo->currentPage, (bufferInfo->isBufferFull ? "YES": "NEIN"), (bufferInfo->prematureFull ? "prematureFlush": "prematureNoFlush"), bufferInfo->minQueryTimeStamp);
	return STATUS_OK;
}

int tree_buffer_info_init(bufferInfo_t* bufferInfo){
	assert(bufferInfo);
	buffer_info_init(bufferInfo);
	return STATUS_OK;
}

double tree_buffer_write_cost(tree_buffer_t* buf){
	/* What would be the estimated cost be if all the entries in this buffer were to be written */
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	return t->accountForWritingWhole();
}


void tree_buffer_read_cost(tree_buffer_t* buf, double* costWithoutCaching, double* realCost, bool doAccount, subtreeDetails_t* details){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	t->accountBufferReadCost(-1, costWithoutCaching, realCost, doAccount, details);
}

struct list_buffer_iterator_t{ /* This should not be used elsewhere .... */
	int index;
	vector<tree_delta_t> sortedList;
	int amountStolen;
    int entriesToAccount;
    internalBuffer_t* internalBuffer; 
	
    void tweakEntriesToAccount(int given){
        entriesToAccount = given;
    }
    
	~list_buffer_iterator_t(){
		/* Return the memory first */
		if (amountStolen >= 0){
			assert(amountStolen > 0);
			if (MAX_HANGING_DELTAS_ALLOWED >= 0){
				MAX_HANGING_DELTAS_ALLOWED += amountStolen;	
				assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
			}
			currentlyStolenAmount -= amountStolen;
			assert(currentlyStolenAmount >= 0);
			checkForBufferMemShrinkage();
		}
		/* Now account for the cost of reading the whole buffer ... */
		double costWithoutCaching, realCost;
        assert(entriesToAccount < 0 || entriesToAccount <= sortedList.size());
        assert(internalBuffer);
		internalBuffer->accountBufferReadCost(entriesToAccount, &costWithoutCaching, &realCost, true, NULL);
		/* fprintf(stderr, "\nAccounting the buffer read cost %lf to the subtree %d", realCost, nodeBeingEmptied); */
		sortedList.clear();
	}

	list_buffer_iterator_t(internalBuffer_t* givenInternalBuffer, int startPage, bool allowedToSteal): index(0), amountStolen(-1), entriesToAccount(-1), internalBuffer(givenInternalBuffer){
        assert(internalBuffer);
		vector<tree_delta_t>* list = &(internalBuffer->deltaList);
		/* Steal the memory from the deltapool first */
        int deltasRequired = -1;
        deltasRequired = MAX_DELTAS_ALLOWED;
        int curSize = (int)(list->size());
        if (curSize - internalBuffer->deltasInCurPage < deltasRequired) deltasRequired = curSize - internalBuffer->deltasInCurPage;
        assert(deltasRequired <= curSize - internalBuffer->deltasInCurPage);
		if (allowedToSteal && deltasRequired > 0){
            checkForBufferMemGrowth(deltasRequired);
			/* fprintf(stderr, "\nStealing memory for %d deltas", deltasRequired); */
			if (MAX_HANGING_DELTAS_ALLOWED >= 0 && numberOfHangingDeltas + deltasRequired > MAX_HANGING_DELTAS_ALLOWED){
				int deltasToFree = numberOfHangingDeltas + deltasRequired - MAX_HANGING_DELTAS_ALLOWED;	
				/* fprintf(stderr, "\nEvicting %d deltas", deltasToFree); */
				evictHangingDelta(deltasToFree, startPage);
			}
			assert(MAX_HANGING_DELTAS_ALLOWED < 0 || numberOfHangingDeltas + deltasRequired <= MAX_HANGING_DELTAS_ALLOWED);
			if (MAX_HANGING_DELTAS_ALLOWED >= 0){
				MAX_HANGING_DELTAS_ALLOWED -= deltasRequired;
				assert(numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
				assert(MAX_HANGING_DELTAS_ALLOWED >= 0 && numberOfHangingDeltas <= MAX_HANGING_DELTAS_ALLOWED);
			}
			amountStolen = deltasRequired;
            currentlyStolenAmount += amountStolen;
            assert(currentlyStolenAmount >= 0);
		}
        assert(sortedList.empty());
 		/* Copy the sorted delta list into a new list prior to sorting. */
		sortedList.insert(sortedList.end(), list->begin(), list->end());
		/* This should be stable sorting because we want to preserve
		 * the order of an equal run of keys inserted into the buffer
		 * */
		stable_sort(sortedList.begin(), sortedList.end(), deltaSorter);

	}

	bool hasNext(){
		return (index < (int)(sortedList.size()));
	}

	int getNext(tree_delta_t* delta, bool dontAdvance){
		if (index >= (int)(sortedList.size())) {
			assert(false);
			return STATUS_FAILED;
		}
		if (delta) memcpy(delta, &sortedList[index], sizeof(tree_delta_t));
		if (!dontAdvance) index++;
		return STATUS_OK;
	}
};

tree_buffer_iterator_t tree_buffer_iterator_create(tree_buffer_t* buf, node_t nodeBeingEmptied, subtreeDetails_t* details){ /* Only for external use when emptying the buffer, not for scanning during query */
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	list_buffer_iterator_t* iterator = new list_buffer_iterator_t(t, listBuf->startPage, true);
	assert(iterator);
	return (tree_buffer_iterator_t)(iterator);
}

void tree_buffer_iterator_destroy(tree_buffer_iterator_t* iterator){
	assert(iterator && *iterator);
	delete (list_buffer_iterator_t*)(*iterator);
	*iterator = NULL;
}

bool tree_buffer_iterator_has_next(tree_buffer_iterator_t* iterator){
	assert(iterator && *iterator);
	list_buffer_iterator_t* iter = (list_buffer_iterator_t*)(*iterator);
	return iter->hasNext();
}

int tree_buffer_iterator_get_next(tree_buffer_iterator_t* iterator, tree_delta_t* delta, bool dontAdvance){
	assert(iterator && *iterator);
	list_buffer_iterator_t* iter = (list_buffer_iterator_t*)(*iterator);
	return iter->getNext(delta, dontAdvance);
}

int tree_buffer_number_insert_entries(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	return (t->info->numberInsertEntries);
}

int tree_buffer_number_entries(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	assert(t->info->numberEntries == (int)(t->deltaList.size()));
	return (t->info->numberEntries);
}

int tree_buffer_get_max_deltas(int mem){
	int r = (mem <= (int)(sizeof(tree_delta_t))) ? 1 : (mem/(int)(sizeof(tree_delta_t)));	
	if (r > MAX_MAX_DELTAS_ALLOWED) return MAX_MAX_DELTAS_ALLOWED;
	else return r;
}

int tree_buffer_find_key(tree_buffer_t* buf, index_key_t key, location_t* loc, double* costWithoutCaching, double* realCost, subtreeDetails_t* details){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
    range_t rangeToFind(key, key);
	vector<flash_bt_data_t> keys;
    if(costWithoutCaching) *costWithoutCaching = 0;
    if(realCost) *realCost = 0;
    bufferInfo_t* info = t->info;
    assert(info);
    if (
            (info->maxKey != INVALID_KEY && COMPARE_KEY(key, info->maxKey) > 0) || 
            (info->minKey != INVALID_KEY && COMPARE_KEY(key, info->minKey) < 0)
       ){
        DEBUG_FILE_PRINTF(stderr, "\nAvoided scanning buffer for %d [%d, %d]", key, info->minKey, info->maxKey);
        return STATUS_FAILED;
    }
    int keysFound = t->findKeysInRange(listBuf->startPage, rangeToFind, costWithoutCaching, realCost, keys, details);    
	if (keysFound > 0){
		if (loc) *loc = keys.front().loc;	
	}
	return (keysFound > 0 ? STATUS_OK : STATUS_FAILED);
}

int tree_buffer_flush(){
	internalBufferMap_t::iterator i;
	for(i = internalBufferMap->begin(); i!= internalBufferMap->end(); i++){
		int startPage = i->first;
		internalBuffer_t* t = i->second;
		int deltasHanging = getAndRemoveHangingBuffer(startPage);
		assert(deltasHanging == t->deltasInCurPage);
		t->flushBuffer();
	}
	ensureNoHanging();
	return STATUS_OK;
}

int tree_buffer_num_buffers(){
	return internalBufferMap->size();
}

int tree_buffer_deltas_in_last_page(tree_buffer_t* buf){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
	return t->deltasInCurPage;
}

int tree_buffer_find_keys_in_range(tree_buffer_t* buf, vector<flash_bt_data_t>& keys, range_t rangeToFind, double* costNoCaching, double* realCost, subtreeDetails_t* details){
	list_buffer_t* listBuf = _get_list_buffer(buf); 
	assert(listBuf);
	assert(internalBufferMap->find(listBuf->startPage) != internalBufferMap->end());
	internalBuffer_t* t = (*internalBufferMap)[listBuf->startPage];
    return t->findKeysInRange(listBuf->startPage, rangeToFind, costNoCaching, realCost, keys, details);    
}

int internalBuffer_t::findKeysInRange(int startPage, range_t rangeToFind, double* costWithoutCaching, double* realCost, vector<flash_bt_data_t>& keys, subtreeDetails_t* details){
	int ret = 0;
	assert((int)(segmentDetails.size()) == info->currentPage);
	assert(info->numberEntries == (int)(deltaList.size()));
	list_buffer_iterator_t* iterator = new list_buffer_iterator_t(this, startPage, false); //We don't need to save the buffer contents per se while scanning it for query -- we just scan it page by page.
    int entriesScanned = 0;
	while(iterator->hasNext()){
		tree_delta_t delta;
		iterator->getNext(&delta, false);
		if (COMPARE_KEY(delta.key, rangeToFind.first) >= 0){
			if (COMPARE_KEY(delta.key, rangeToFind.second) <= 0){
				ret ++;
				keys.push_back(flash_bt_data_t(delta.key, delta.loc));
			}
		}
        entriesScanned += 1;
	}
    iterator->tweakEntriesToAccount(entriesScanned);
	delete iterator;
	iterator = NULL;
	return ret;
}
