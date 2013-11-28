
#ifndef STATUS_CODES_H
#define STATUS_CODES_H

enum {
    STATUS_INVALID = -1,
    STATUS_OK = 0,
    STATUS_NO_FREE,
    STATUS_NOT_AVAIL,
    STATUS_NOT_PERMITTED,
    STATUS_FAILED
};

#define OUT_OF_MEMORY (int)(1)
#define OUT_OF_FLASH (int)(2)

#endif
