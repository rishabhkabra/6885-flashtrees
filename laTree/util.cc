#include "util.h"
using namespace std;

int timeval_subtract (struct timeval* result, struct timeval* x, struct timeval* y)
{
	/* Perform the carry for the later subtraction by updating y. */
	if (x->tv_usec < y->tv_usec) {
		int nsec = (y->tv_usec - x->tv_usec) / ONE_MILLION + 1;
		y->tv_usec -= ONE_MILLION * nsec;
		y->tv_sec += nsec;
	}
	if (x->tv_usec - y->tv_usec > ONE_MILLION) {
		int nsec = (x->tv_usec - y->tv_usec) / ONE_MILLION;
		y->tv_usec += ONE_MILLION * nsec;
		y->tv_sec -= nsec;
	}

	/* Compute the time remaining to wait.
	 *           tv_usec is certainly positive. */
	result->tv_sec = x->tv_sec - y->tv_sec;
	result->tv_usec = x->tv_usec - y->tv_usec;
	assert(result->tv_usec < ONE_MILLION);
	/* Return 1 if result is negative. */
	return x->tv_sec < y->tv_sec;
}

double timeval_to_seconds(struct timeval* tv){
	return (tv->tv_usec/((double)(ONE_MILLION)) + tv->tv_sec);
}

double nowTime(){
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return timeval_to_seconds(&tv);
}

void* getPageAlignedBuffer(int size, void** actualAddress){
	int pagesize=getpagesize();
	assert(size >= 0);
	void* realbuffer=malloc(size + pagesize);
	ptrdiff_t aligned = (ptrdiff_t)((uint8_t*)realbuffer+pagesize-1);
	aligned /= pagesize;
	aligned *= pagesize;
	void* alignedbuffer= (void*)aligned;
    if (actualAddress) *actualAddress = realbuffer;
    return alignedbuffer;
}
