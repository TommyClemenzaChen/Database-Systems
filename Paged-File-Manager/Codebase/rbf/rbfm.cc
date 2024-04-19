#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{

}

RC RecordBasedFileManager::createFile(const string &fileName) {

    if (_pf_manager->createFile(fileName) != 0) {
        return -1;
    }

    void *initial_data = createRBPage();

    //add page to file handler
    FileHandle handle;
    _pf_manager->openFile(fileName, handle);
    handle.appendPage(initial_data);
    _pf_manager->closeFile(handle);
    
    free(initial_data);

    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
   

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

void* RecordBasedFileManager::createRBPage() {
    //allocate space for page
    void* rbpage = malloc(PAGE_SIZE);

    //create slot directory
    SlotDirectory slot_directory;
    slot_directory.numSlots = 0;
    slot_directory.offset = PAGE_SIZE;

    memcpy(rbpage, &slot_directory, sizeof(slot_directory));

    return rbpage;
}

unsigned RecordBasedFileManager::calculateRecordSize(const vector<Attribute> &recordDescriptor) {
        

}


bool RecordBasedFileManager::isNull(unsigned null_flags, unsigned shift){
    int correctedShift = 8 - shift;
    return (1 << (8 - correctedShift)) & null_flags == (1 << (8 - correctedShift));
}

SlotDirectory RecordBasedFileManager::getSlotDirectory(void *pageData) {
    SlotDirectory slot_directory;
    memcpy(&slot_directory, pageData, sizeof(SlotDirectory));
    return slot_directory;
}

