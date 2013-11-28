#ifndef UTIL_H
#define UTIL_H

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
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <fstream>
#include <set>
#include <iomanip>
#include <vector>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <stdint.h>


int timeval_subtract (struct timeval* result, struct timeval* x, struct timeval* y);
double nowTime();
double timeval_to_seconds(struct timeval* tv);
void* getPageAlignedBuffer(int size, void** actualAddress);
#define ONE_MILLION ((int)(1e6))
#endif
