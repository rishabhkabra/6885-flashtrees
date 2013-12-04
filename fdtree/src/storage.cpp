/*-------------------------------------------------------------------------
 *
 * storage.cpp
 *	  Storage manager interface routines
 *
 * The license is a free non-exclusive, non-transferable license to reproduce, 
 * use, modify and display the source code version of the Software, with or 
 * without modifications solely for non-commercial research, educational or 
 * evaluation purposes. The license does not entitle Licensee to technical support, 
 * telephone assistance, enhancements or updates to the Software. All rights, title 
 * to and ownership interest in the Software, including all intellectual property rights 
 * therein shall remain in HKUST.
 *
 * IDENTIFICATION
 *	  FDTree: storage.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include "storage.h"
#include "error.h"
#ifdef __APPLE__
#define O_DIRECT -1
#endif

//data path for storing data
char DATAPATH[256] = "../data";

int file_get_new_fid()
{
	static int curFileId = 100;
	return curFileId ++;
}

void print_page(Page page)
{
	printf("page: \n");
	Entry * data = PAGE_DATA(page, Entry);
	for (int i = 0; i < PAGE_NUM(page); i++)
		printf("%d, ", data[i].key);
	printf("\n");
}

FilePtr file_open(int fid, bool isExist)
{
	char path[256];
	sprintf(path, "%s/%d.dat", DATAPATH, fid);
	printf("opening file %s\n", path);
	
	HANDLE fhdl;
	//FILE* fp = fopen(path, "r");
	
	if (isExist) {
	  fhdl = open(path, O_RDWR);// | O_DIRECT);
	} 
	else {
	  fhdl = open(path,  O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);	  
	  }

	//unsigned int fflag = 0;
	//fflag |= FILE_FLAG_NO_BUFFERING|FILE_FLAG_WRITE_THROUGH;
	
	//HANDLE fhdl = open(path, flags, 0644); //O_RDWR | O_CREAT, 0644);//flags);

	/*
	HANDLE fhdl = CreateFileA( path,
                            GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ,
                            NULL,
                            openflag,
                            fflag,
                            NULL);
	*/
	if  (fhdl == INVALID_HANDLE_VALUE)
	  {
	    elog(ERROR, "ERROR: FileOpen failed (error=%d)\n");//, GetLastError());
	    exit(1);
	  }
	return fhdl;
}

int file_close(FilePtr fhdl)
{
	if (close(fhdl) != 0)
	  {
	  elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n");//, GetLastError());
	  exit(1);
	}
	return 1;
}

void file_seek(FilePtr fhdl, long offset)
{
  if (lseek(fhdl, offset, SEEK_SET) == -1)
    {
      elog(ERROR, "ERROR: SetFilePointerEx failed (error=%d)\n");//, GetLastError());
      exit(1);
    }
}

DWORD file_read(FilePtr fhdl, Page buffer, long num)
{
	DWORD nread;

	nread = read(fhdl, buffer, num);
	if (nread == -1)
	{
	  elog(ERROR, "ERROR: FileRead I/O failed, winerr=%d\n");//, GetLastError());
	  exit(1);
	}
	return nread;
}

DWORD file_write(FilePtr fhdl, Page buffer, long num)
{
	DWORD nwrite;
	nwrite = write(fhdl, buffer, num);
	if (nwrite == -1 || nwrite != num)
	{
	  elog(ERROR, "ERROR: FileWrite I/O failed, winerr=%d\n");//, GetLastError());
	  exit(1);
	}
	return nwrite;
}

void file_flush(FilePtr fhdl)
{
  //fclean(fhdl);
}

void file_delete(FilePtr fhdl, int fid)
{
	char path[256];
	sprintf(path, "%s/%d.dat", DATAPATH, fid);

	if (close(fhdl) != 0)
	{
	  elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n");//, GetLastError());
	  exit(1);
	}
	
	if (unlink(path) != 0)
	  {
	    elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n");//, GetLastError());
	    exit(1);
	  }
	
}

bool file_clearDataDir() 
{  
  /*
	WIN32_FIND_DATA finddata;  
	HANDLE hfind;
	int fid;
	const char * path = DATAPATH;
	char * pdir;  

	pdir = new char[strlen(path)+5];  
	strcpy( pdir, path);  
	if( path[strlen(path)-1] != '\\')  
		strcat(pdir,"\\*.*");  
	else  
		strcat(pdir,"*.*");  

	hfind = FindFirstFile(pdir,&finddata);  
	if (hfind == INVALID_HANDLE_VALUE)  
		return FALSE;  

	delete []pdir;  
	do
	{
		if (sscanf(finddata.cFileName, "%d.dat", &fid) < 1)
			continue;

		pdir = new char[strlen(path) + strlen(finddata.cFileName) + 2];  
		sprintf(pdir,"%s/%s",path,finddata.cFileName);		

		//if (strcmp(finddata.cFileName,".") == 0 || strcmp(finddata.cFileName,"..") == 0)  
		//{
		//	RemoveDirectory(pdir);
		//	continue;
		//}
		if ((finddata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0)  
			DeleteFile(pdir);

		delete [] pdir;
	}while (FindNextFile(hfind,&finddata));  

	//if (RemoveDirectory(path))
	//	return TRUE;
	//else
	//	return FALSE;
	*/
	return true;
}
