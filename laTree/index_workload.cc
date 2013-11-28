#include "flash.h"

#include "tree_structures.h"
#include <subtree_tree.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>
#include <math.h>
#include "index_func.h"
#include "util.h"

using namespace std;

#define SPECIAL_FILE_MARK 'X'

char* outputFileName = NULL;
char* OUTPUT_FILE_NAME = NULL;

bool actualIndexOpsStarted = false;
bool doIoLogging = false;
int nQueries = 0, nKeysInserted = 0;
int curTimeStamp = -1;

/* The below parameter controls the number of keys we want to be actually inserted
 * into the La-Tree's tree (as opposed to in its buffers. Setting it too high
 * will introduce end-effects and too low will make it cheating. */

/* Its setting does not make any impact if the workload is large (~1M) even if
 * this value = 1.0. But it only makes sense when the workload is small that
 * edge effects matter */

const double FINAL_FLUSH_THRESH = 0.75; 

bool doWeWriteAtSamePlace = false;
extern int numberOfActualNodes;
bool ssdMode = false;

extern int keysActuallyInserted; //costItem

void zeroAdditionalCounters(){
	keysActuallyInserted=0; //costItem
}

void zeroCounters(){
	FlashZeroCounters();
	zeroAdditionalCounters();
}

enum operation_t {
    QUERY_OPERATION = 0,
    INSERT_OPERATION = 1,
    RANGE_QUERY_OPERATION = 2,
    INVALID_OPERATION = 3
};

struct treeWorkload_t{
	operation_t operation;
	index_key_t key;
	index_key_t auxkey;
	treeWorkload_t(index_key_t key = INVALID_KEY, operation_t operation = INVALID_OPERATION): operation(operation), key(key), auxkey(INVALID_KEY) {}
};

uint32_t largestPossibleKeyMask = (uint32_t)((0x1U << (8 * sizeof(index_key_t) - 1)) - 1);

int readWorkloadLine(FILE* fp, treeWorkload_t* unit){
    int ret;
    index_key_t key, auxkey;
    operation_t operation;
    ret = fscanf(fp, "%d", &operation);
    if (ret < 1){
        fprintf(stderr, "\nOops -- failed to read line fully");
        return STATUS_FAILED;
    }
    ret = fscanf(fp, "%d", &key);
    if (ret < 1){
        fprintf(stderr, "\nOops -- failed to read line fully");
        return STATUS_FAILED;
    }
    assert(key <= largestPossibleKeyMask);
    unit->key = key;
    unit->operation = operation;
    if (operation == RANGE_QUERY_OPERATION){
        ret = fscanf(fp, "%d", &auxkey);
        if (ret < 1){
            fprintf(stderr, "\nOops -- failed to read line fully");
            return STATUS_FAILED;
        }
        unit->auxkey = auxkey;
    }
    return STATUS_OK;
}

treeWorkload_t getRandomWorkItem(bool initialInsertion, char* uniformWorkloadArgsStr){
	double qr; 
	int range;
	int ret = sscanf(uniformWorkloadArgsStr, "%lf,%d", &qr, &range);
	assert(ret == 2);
	int randValue = 1 + (int) ((double)(range) * (rand() / (RAND_MAX + 1.0)));	
	bool isQuery = false;
	if (!initialInsertion){
		double sample = rand()/(RAND_MAX + 1.0);
		assert(sample <= 1.0 && sample > 0.0);
		if (sample <= qr){
			isQuery = true;
		}
	}
	treeWorkload_t work(randValue, isQuery ? QUERY_OPERATION : INSERT_OPERATION);
	return work;
}

void generateWorkload(vector<treeWorkload_t>& v, vector<treeWorkload_t>& initialInsertion, char* inputFileName, bool& isUniformWorkload, int& numKeysInitial, int& numKeysActual, string& uniformWorkloadArgs){
	assert(inputFileName[0] == SPECIAL_FILE_MARK);
	double queryRatio;
	int numberOfValues = 0, range = 0, numberOfInitial = 0;
	char fileName[255];
	sscanf(inputFileName + 1, "%d-%d-%lf-%d-%s", &numberOfValues, &numberOfInitial, &queryRatio, &range, fileName);
    assert(numberOfValues > 0);
	vector<int> given;
	if (strcasecmp(fileName, "nil") != 0){
		FILE* f = fopen(fileName, "r");
		assert(f);
		while(!feof(f)){
			int d;
			fscanf(f, "%d", &d);
			given.push_back((abs(d) + 1)& (largestPossibleKeyMask));
		}
		fclose(f);
		fprintf(stderr, "\nLoaded up %d values", given.size());
	}
	vector<int> givenCopy(given);
	assert(numberOfValues > 0 && numberOfInitial >= 0);
	v.clear();
	initialInsertion.clear();
	isUniformWorkload = false;
	uniformWorkloadArgs = "";
	queryRatio = queryRatio/(1.0 + queryRatio); //The query ratio is the rato of reads to writes
	assert(queryRatio <= 1.0 && queryRatio >= 0);
	int numToQuery = queryRatio == 0 ? -1 : (int)(ceil(1.0/queryRatio));
	assert(numToQuery < 0 || numToQuery >= 1);
    int numQueries = 0;
	int interQuery = 0;
	int sumInterQuery = 0;
	/* given: The original sequence of values -- used for insertions */
	/* givenCopy: The shuffled sequence of values  -- used for deciding what to query */
	if (givenCopy.empty()){//uniform workload
		numKeysActual = numberOfValues;
		numKeysInitial = numberOfInitial;
		isUniformWorkload = true;
		char uArg[255];
		sprintf(uArg, "%lf,%d", (double)queryRatio, (int)range);
		uniformWorkloadArgs = string(uArg);
		return;
	}

	for(int i = 0;i< numberOfValues + numberOfInitial; i++){
		bool initialDone = (i >= numberOfInitial) ? true : false;
		int randValue = 0;
		if (given.empty()){
			randValue = 1 + (int) ((double)(range) * (rand() / (RAND_MAX + 1.0)));	
			assert(randValue <= range && randValue >= 1);
		}else{
			int j = i % given.size();
			randValue = given.at(j);
		}
		bool isQuery = false;
		if (initialDone){
			double sample = rand()/(RAND_MAX + 1.0);
			assert(sample <= 1.0 && sample > 0.0);
			if (sample <= queryRatio){
				isQuery = true;
			}

			if (isQuery){
				numQueries++;
				sumInterQuery += interQuery;
				interQuery = 0;
				if (!given.empty()){
					randValue = givenCopy.at(i % givenCopy.size());
				}
			}else{
				interQuery ++;
			}
		}
		operation_t otherOp = INSERT_OPERATION;
		treeWorkload_t work(randValue, isQuery ? QUERY_OPERATION : otherOp);
		if (initialDone) v.push_back(work);
		else initialInsertion.push_back(work);
	}
	fprintf(stderr, "\nGenerated %lf query fraction with avg interquery being %lf", ((double)numQueries)/numberOfValues, (double)sumInterQuery/numQueries);
}

void getMeanStdDev(vector<int>& list, int& mean, int& stddev){
	unsigned long long sum = 0;
	unsigned long long sumSq = 0;
	unsigned long long n = list.size();
	assert(n > 1);
	int i;
	for(i = 0;i < int(n);i++){
		int time = list[i];
		fprintf(stderr, "\nSaw the query time %d", time);		
		sum+=time;
		sumSq += time* time;
	}
	assert(sumSq*n - sum*sum >= 0);
	assert(sum >= 0 && sumSq >= 0);
	stddev = (int)(sqrt((double)((n*sumSq - sum*sum)/(n*(n-1)))));
	mean = sum/n;
}

int main(int argc, char** argv){
	int i;
	setbuf(stdout, NULL);
	if (argc<11){
		fprintf(stderr, "\nUsage: %s <flashParamFile> <nodeSize> <bufferSize> <numNodesCached> <inputFile> <initialInputFile> <output_file> <seed> <ssdMode> <fanout> <subtreeHeight>", argv[0]);
		fprintf(stderr, "\n");
		exit(1);
	}

	char* inputFileName = strdup(argv[5]);
	char* initialInputFileName = strdup(argv[6]);
	OUTPUT_FILE_NAME = (outputFileName = strdup(argv[7]));
	assert(outputFileName);
	int seed = atoi(argv[8]);
	fprintf(stderr, "\nSeeding with %d", seed);
	srand(seed);
	ssdMode = (atoi(argv[9]) == 1);
    int fanout = atoi(argv[10]);
    int subtreeHeight = atoi(argv[11]);
    if (ssdMode){
        doWeWriteAtSamePlace = doIoLogging = true;
    }
	char structuralDetails[255];
    snprintf(structuralDetails, sizeof(structuralDetails), "%d-%d", fanout, subtreeHeight);
	int ret;
    char* flashFileParams = strdup(argv[1]);
	int nodeSize = atoi(argv[2]);
	int bufferSize = atoi(argv[3]);
	int numNodesCached = atoi(argv[4]);
	flashLoadCosts(flashFileParams);
	FlashInit();
	void* index = NULL;
	index_func_t indexFuncs;
	memset(&indexFuncs, 0, sizeof(indexFuncs));
	vector<treeWorkload_t> v;
	vector<treeWorkload_t> initialInsertion;
	bool isUniformWorkload = false;
	string uniformWorkloadArgs = "";
	int numKeysInitial = 0, numKeysActual = 0;
	if (inputFileName[0] != SPECIAL_FILE_MARK){
		FILE* inputFile = fopen(inputFileName, "r");
		assert(inputFile);
		while(!feof(inputFile)){
            treeWorkload_t unit;
            int ret = readWorkloadLine(inputFile, &unit);
			if (ret != STATUS_OK){
                fprintf(stderr, "\nFailed to parse a line in the inputfile");
                continue;
            }
			v.push_back(unit);
		}
		fclose(inputFile);
        if (strcmp(initialInputFileName, "nil") != 0){
            FILE* inputFile = fopen(initialInputFileName, "r");
            assert(inputFile);
            while(!feof(inputFile)){
                treeWorkload_t unit;
                int ret = readWorkloadLine(inputFile, &unit);
                if (ret != STATUS_OK){
                    fprintf(stderr, "\nFailed to parse a line in the initial inputfile");
                    continue;
                }
                unit.operation = INSERT_OPERATION;
                v.push_back(unit);
            }
            fclose(inputFile);
        }
	}else generateWorkload(v, initialInsertion, inputFileName, isUniformWorkload, numKeysInitial, numKeysActual, uniformWorkloadArgs);
	if (!isUniformWorkload){
		fprintf(stderr, "\nLoaded up %d values", v.size());
		assert(v.size() > 0);
	}else{
		assert(v.empty() && initialInsertion.empty() && !uniformWorkloadArgs.empty());
	}
	latree_describe_self(&indexFuncs);
	assert(indexFuncs.indexSize > 0);
	index = malloc(indexFuncs.indexSize);
	ret = indexFuncs.init_index(index, nodeSize, bufferSize, numNodesCached, numKeysActual, structuralDetails);
	assert(ret == STATUS_OK);
	location_t defaultLoc = INVALID_LOCATION;
	nQueries = 0, nKeysInserted = 0;
	int abortReason = 0;
	char* uniformWorkloadArgsStr = strdup(uniformWorkloadArgs.c_str());
	double tStart, tBulkLoad = 0, tActual = 0, tFlush = 0;
	tStart = nowTime();
	if (numKeysInitial > 0){
        fprintf(stderr, "\nDoing the bulk loading ... with %d values", initialInsertion.size());
		for(i= 0; i< numKeysInitial; i++){
			treeWorkload_t work = isUniformWorkload ? getRandomWorkItem(true, uniformWorkloadArgsStr) : initialInsertion.at(i);
			assert(work.operation == INSERT_OPERATION);
			index_key_t key = work.key;
			defaultLoc = key; /* Hoping that the compiler would convert the index_key_t rhs into the lhs */
			ret = indexFuncs.put_value(index, key, &defaultLoc, &abortReason);
			assert(ret == STATUS_OK);
			assert(abortReason == 0);
		}
		indexFuncs.flush_index(index);
		ret = indexFuncs.verify_tree(index);
		assert(ret == STATUS_OK);
		fprintf(stderr, "\nFlushed and verified the initially bulk loaded index");
		indexFuncs.loading_done(index);
	}
	zeroCounters();
	tBulkLoad = nowTime() - tStart;
	int actualIndexLoad = 0; /* .... how much stuff was actually carried out */
    actualIndexOpsStarted = true;

	tStart = nowTime();

	for(i= 0; i< numKeysActual; i++){
		treeWorkload_t workItem = isUniformWorkload ? getRandomWorkItem(false, uniformWorkloadArgsStr) : v[i];
		index_key_t key = workItem.key;
		index_key_t auxkey = workItem.auxkey;
		defaultLoc = key; /* Hoping that the compiler would convert the index_key_t rhs into the lhs */
        operation_t op = workItem.operation;
        /* Range query decls */
        vector<flash_bt_data_t> range_keys;
        range_t range(key, auxkey);
        switch(op){
            case RANGE_QUERY_OPERATION:
                indexFuncs.get_range(index, &range, range_keys);
                nQueries ++;
                break;
            case INSERT_OPERATION:
                ret = indexFuncs.put_value(index, key, &defaultLoc, &abortReason);
                nKeysInserted++;
                assert(ret == STATUS_OK);
                assert(abortReason == 0);
                break;
            case QUERY_OPERATION:
                ret = indexFuncs.get_value(index, key, NULL);
                nQueries ++;
                assert(ret == STATUS_OK);
                break;
            default:
                fprintf(stderr, "\nUnknown operation %d -- error", op);
                exit(1);
        }
        actualIndexLoad ++;
        curTimeStamp ++;
	}
	tActual = nowTime() - tStart;

	fprintf(stderr,"\nInserted and queried values, ensuring that enough keys are inserted into the tree ..");

    if (keysActuallyInserted < (int)((FINAL_FLUSH_THRESH) * nKeysInserted)){
        fprintf(stderr, "\nVery less keys actually inserted -- %d (out of %d), flushing buffers .. ", keysActuallyInserted, nKeysInserted);
		tStart = nowTime();
        ret = indexFuncs.flush_query_buffers(index, true); 
		tFlush = nowTime() - tStart;
        assert(ret == STATUS_OK);
    }

	ret = indexFuncs.verify_tree(index);
	assert(ret == STATUS_OK);
	int height = indexFuncs.get_height(index);
	int nkeysInNode = indexFuncs.get_nkeys(index);
	assert(height >= 1 && nkeysInNode >= 0);
	int numBuffers = -1; 
	numBuffers = indexFuncs.get_num_buffers(index);
	ret = indexFuncs.destroy_index(index);
	assert(ret == STATUS_OK);
	FILE* of = fopen(outputFileName, "w");
	assert(of);
    fprintf(of, "\n\n");
    fprintf(of, "# actualIndexLoad nQueries nKeysInserted nodeSize bufferSize numNodesCached height nkeysInNode numBuffers keysActuallyInserted\n");
	fprintf(of, "%d %d %d %d %d %d %d %d %d %d\n", actualIndexLoad, nQueries, nKeysInserted, nodeSize, bufferSize, numNodesCached, height, nkeysInNode, numBuffers, keysActuallyInserted);
	FlashDump(of);
	fclose(of);
	FlashDestroy();
	if (outputFileName){
		free(outputFileName);
		outputFileName = NULL;
	}
	if (inputFileName){
		free(inputFileName);
		inputFileName = NULL;
	}
	if (flashFileParams){
		free(flashFileParams);
		flashFileParams = NULL;
	}
	return 0;
}
