#ifdef NO_PRINTF
#define DEBUG_FILE_PRINTF(...) do{} while(0)
#else
#define DEBUG_FILE_PRINTF fprintf
#endif
