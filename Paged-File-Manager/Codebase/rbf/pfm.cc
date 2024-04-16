#include "pfm.h"
#include <stdio.h>
#include <sys/stat.h>


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

// copy constructor
PagedFileManager::PagedFileManager()
{

}

// destructor
PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    struct stat buffer;
    if(stat(fileName.c_str(), &buffer) == 0){
        return -1;
    }
    FILE * f = fopen(fileName.c_str(), "w");
    fclose(f);

    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    struct stat buffer;
    if(stat(fileName.c_str(), &buffer) == -1){
        return -1;
    }
    remove(fileName.c_str());
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    //check if file exists
    struct stat buffer;
    if(stat(fileName.c_str(), &buffer) == -1) {
        return -1;
    }

    //check if fileHandler exists for current file
    if (fileHandle.getFilePointer() != nullptr) {
        return -1;
    }

    //create file handler for current file
    FILE *fp = fopen(fileName.c_str(), "rb+");
    if (fp == NULL) {
        return -1;
    }

    //set file handler
    fileHandle.setFilePointer(fp);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE* fp = fileHandle.getFilePointer();
    if (fp == nullptr) {
        return -1;
    }

    fflush(fp);
    fclose(fp);

    //reset fileHandle
    fileHandle.setFilePointer(nullptr);
    
    return 0;
}

// only increment for a success file
FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
    _fp = nullptr;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    //check if page exists (-1 if false)
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }

    //move cursor to correct page (start pos)
    if (fseek(_fp, PAGE_SIZE * pageNum, SEEK_SET) != 0) {
        return -1;
    }

    size_t bytes_read = fread(data, 1, PAGE_SIZE, _fp);
    if (bytes_read < PAGE_SIZE) {
        return -1;
    }
    
    readPageCounter = readPageCounter + 1;
    return 0;
}

// Figure out free space, do the insertion. Then find the next page with a free space
// Create a new page if there is no space
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= getNumberOfPages()) {
        return -1;
    }

    //move cursor to correct page (start pos)
    if (fseek(_fp, PAGE_SIZE * pageNum, SEEK_CUR) != 0) {
        return -1;
    }
    
    size_t bytes_written = fwrite(data, 1, PAGE_SIZE, _fp);
    if (bytes_written < PAGE_SIZE) {
        return -1;
    }

    fflush(_fp);

    writePageCounter = writePageCounter + 1;

    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if (fseek(_fp, 0, SEEK_END) != 0) {
        return -1;
    }
    size_t bytes_written = fwrite(data, 1, PAGE_SIZE, _fp);
    if (bytes_written < PAGE_SIZE) {
        return -1;
    }

    fflush(_fp);
    appendPageCounter = appendPageCounter + 1;
     
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    //find total bytes
    fseek(_fp, 0, SEEK_END);
    int total_bytes = ftell(_fp);
    
    return total_bytes / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;

	return 0;
}

FILE* FileHandle::getFilePointer() {
    return _fp;
}

FILE* FileHandle::setFilePointer(FILE* fp) {
    _fp = fp;
}