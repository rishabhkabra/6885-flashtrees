#include "raw_metadata.h"
using namespace std;

bool doDirectIo = true;
bool doLogging = false;
bool doInteractive = false;
bool traceMode = false;
FILE* outputTraceFile = NULL;

int main(int argc, char **argv){
    setbuf(stdout, NULL);
    int numFixedArgs = 4;
    if (argc < numFixedArgs){
        fprintf(stderr, "\nUsage: %s <rawDisk> <traceFile> <blockSize> <outputFile>", argv[0]);
        exit(1);
    }
    char* diskFile = argv[1];
    char* traceFile = argv[2];
    int blockSize = atoi(argv[3]);
    char* outputFileName = strdup(argv[4]);
    fprintf(stderr, "\nGot the block size as %d", blockSize);
    argv += numFixedArgs;
    argc -= numFixedArgs;
    char ch;
    while ((ch = getopt(argc, argv, "rdit:")) != EOF){
        switch(ch) {
            case 'd':
                doDirectIo = false;
                fprintf(stderr, "\nNot doing direct io");
                break;
            case 'i':
                doInteractive = true;
                fprintf(stderr, "\nDoing interactive");
                break;
            case 't':
                traceMode = true;
                fprintf(stderr, "\nDoing tracemode");
				outputTraceFile = fopen(optarg, "w");
				assert(outputTraceFile);
                break;
            case 'r':
                doLogging = true;
                fprintf(stderr, "\nDoing logging");
                break;
			default:
				fprintf(stderr, "\nIncorrect option ... %c", ch);
				exit(1);
			}
	}
    FILE* inputFile = fopen(traceFile, "r");
    assert(inputFile);
    int lineNum = 0;
    address_t firstWriteByte = -1;
    traceMetadata_t tracer(diskFile, blockSize, doDirectIo, firstWriteByte, doLogging, traceMode);
    int j = 0;
    int snapshotTime = 50000;
    int opsDid = 0;
    double t1 = nowTime();
    while(!feof(inputFile)){
        lineNum ++;
        char opChar[100];
        int pgSize;
        workDetails_t unit;
        int ret = fscanf(inputFile, "%s %lld %lld %d", opChar, &unit.size, &unit.startByte, &pgSize);
        if (ret < 4){
            fprintf(stderr, "\nInvalid line ... ignoring %d (got only %d)", lineNum, ret);
            continue;
        }
        unit.opType = (opChar[0] == 'W') ? WRITE_OP : READ_OP;
        if (firstWriteByte < 0 && unit.opType == WRITE_OP) firstWriteByte = unit.startByte;
        tracer.takeTrace(unit);
        opsDid++;
        if (j++ == snapshotTime){
             j = 0;
             fprintf(stderr, ".");
        }
    }
    double t2 = nowTime();
#if 0
	if (doInteractive){
		while(1){
			char ans;
			fprintf(stdout, "\nInteractive mode ... Press y/n to continue/stop\n");
            fflush(stdout);
			fscanf(stdout, "%c", &ans);
			if (ans == 'y' || ans == 'Y') break;
			else if (ans == 'n' || ans == 'N') exit(0);
		}
	}
#endif
    fprintf(stdout, "\nRunning %d operations took %lf seconds", opsDid, t2 - t1);
    if (!traceMode){
        assert(outputFileName);
        FILE* of = fopen(outputFileName, "w");
        if (!of){
            of = stdout;
            fprintf(stderr, "\nCouldn't open output file %s, dumping to stdout", outputFileName);
        }
        ioDetails_t& counts = tracer.ioCounters;
        fprintf(of, "%d %d %d %d %d\n", counts.numTotalOps, counts.numTotalReads, counts.numTotalWrites, counts.numRandomReads, counts.numRandomWrites);
        fprintf(of, "%lf %lf\n", tracer.ioTimeSoFar, t2 - t1);
        if (of != stdout){
            fclose(of);
        }
        outputFileName = NULL;
        free(outputFileName);
    }
	if (outputTraceFile){
		fclose(outputTraceFile);
		outputTraceFile = NULL;
	}
}
