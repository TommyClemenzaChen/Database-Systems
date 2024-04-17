#include "rbfm.h"
#include "pfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

// copy constructor
RecordBasedFileManager::RecordBasedFileManager(){}

// destructor
RecordBasedFileManager::~RecordBasedFileManager(){}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // create page here and create slot directory
    
    // store number of records and the free space offset
    return PagedFileManager.createFile(&fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager.destroyFile(&fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager.openFile(&fileName, &fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager.closeFile(&fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
