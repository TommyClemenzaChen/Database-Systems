#include "rbfm.h"
#include <cmath>

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

    void *pageData = malloc(PAGE_SIZE);
    createRBPage(pageData);

    //add page to new file
    FileHandle handle;
    _pf_manager->openFile(fileName, handle);
    handle.appendPage(pageData);
    _pf_manager->closeFile(handle);
    
    free(pageData);

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
    /*NOTE: *data is the new record to insert
    ex. ("Tom", 25, NULL, NULL, 100) = "[0x30][0x04][0x03][0x04]"
    */
    void *pageData = malloc(PAGE_SIZE);

    unsigned recordSize = calculateRecordSize(recordDescriptor);
    unsigned totalDataSize = recordSize + sizeof(Slot);
    
    bool pageFound = false;
    unsigned pageNum;

    SlotDirectory sd;
    unsigned freeSpace;
    for (int i = 0; i < fileHandle.getNumberOfPages(); i++) {
        
        fileHandle.readPage(i, pageData);
        
        sd = getSlotDirectory(pageData);
        freeSpace = getFreeSpace(pageData);
        
        if (totalDataSize < freeSpace) {
            pageNum = i;
            pageFound = true;
            break;
        }
    }

    
    //we didn't find page :(
    if (!pageFound) {
        void *new_pageData = malloc(PAGE_SIZE);
        createRBPage(new_pageData);
        pageData = new_pageData;
        sd = getSlotDirectory(new_pageData);
        freeSpace = getFreeSpace(new_pageData);
        pageNum = fileHandle.getNumberOfPages();
        free(new_pageData);
    }

    cout << "Free Space: " << freeSpace << endl;
    cout << "Page num: " << pageNum << endl;

    //update slot directory of the page we're using
    sd.FSO -= totalDataSize;
    sd.numSlots += 1;

    memcpy(pageData, &sd, sizeof(SlotDirectory));

    //add new slot (or find first empty?)
    Slot newSlot;
    newSlot.RO = sd.FSO - recordSize;
    newSlot.size = recordSize;

    //find slot data position and insert
    void* slotData = (char*)pageData + sizeof(SlotDirectory) + (sd.numSlots * sizeof(Slot));

    memcpy(slotData, &newSlot, sizeof(Slot));

    //find start of next record position and insert
    void* recordData = (char*)slotData + sd.FSO;
    memcpy(recordData, data, recordSize);

    //set RID
    rid.pageNum = pageNum;
    rid.slotNum = sd.numSlots;

    fileHandle.writePage(pageNum, pageData);

    free(pageData);
    
    return 0;

}   

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    //read entire page
    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, pageData);

    //goto correct slot
    Slot slot;
    void *slotData = (char*)pageData + sizeof(SlotDirectory) + (rid.slotNum * sizeof(Slot));
    memcpy(&slot, slotData, sizeof(Slot));

    void *recordData = (char*)pageData + slot.RO;
    memcpy(data, recordData, slot.size);

    free(pageData);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
cout << data << endl;
    return 0;
}

RC RecordBasedFileManager::createRBPage(void *pageData) {
    //create slot directory
    SlotDirectory slot_directory;
    slot_directory.numSlots = 0;
    slot_directory.FSO = PAGE_SIZE;

    //write slot_directory to page
    memcpy(pageData, &slot_directory, sizeof(SlotDirectory));
    
    return 0;
}

bool RecordBasedFileManager::isNull(unsigned null_flags, unsigned shift){
    int correctedShift = 8 - shift;
    return (1 << (8 - correctedShift)) & null_flags == (1 << (8 - correctedShift));
}

void* readNBytes(const void* data, size_t& offset, size_t n) {
    void* buffer = new char[n];
    memcpy(buffer, static_cast<const char*>(data) + offset, n);
    offset += n;

    return buffer;
}

unsigned RecordBasedFileManager::calculateRecordSize(const vector<Attribute> &recordDescriptor, void* data) {
    unsigned recordSize = 0;
    size_t offset = 0;
    int bytesToRead = ceil(&recordDescriptor.size()/8);
    int strLen = 0;
    
    // isNull(unsigned null_flags, unsigned shift)
    
    // read in bytesToRead bytes from *data and store it in null flags
    // cast to unsigned
    unsigned null_flags = static_cast<unsigned>(readNBytes(data, offset, bytesToRead));

    for (int i = 0; i < recordDescriptor.size(); i++) {

        if (isNull(null_flags, i)) continue;

        Attribute attr = recordDescriptor[i];
        switch (attr.type)  {
            //only add bytes to recordSize if not NULL
            case TypeInt:
                // 4 bytes
                recordSize += 4;
                // read in 4 bytes from the data stream
                readNBytes(data, offset, 4);
                break;
            case TypeReal:
                //4 bytes
                recordSize += 4;
                // read in 4 bytes from the data stream
                readNBytes(data, offset, 4);
                break;
            case TypeVarChar:
                // read in first 4 bytes from the data stream (will tell us strlen)
                strLen = static_cast<int>(readNBytes(data, offset, 4)); // cast to int for calculating
                readNBytes(data, offset, strLen); // actually read strlen bytes from the stream
                recordSize += 4+strLen;
        }
        return recordSize;
    }

}

SlotDirectory RecordBasedFileManager::getSlotDirectory(void *pageData) {
    SlotDirectory slot_directory;
    memcpy(&slot_directory, pageData, sizeof(SlotDirectory));
    return slot_directory;
}

unsigned RecordBasedFileManager::getFreeSpace(void *pageData) {
    unsigned totalRecordSpace = 0;
    
    SlotDirectory sd = getSlotDirectory(pageData);
    
    //HEADER = [SD][slot 1][slot 2][slot 3]...[slot n]
    unsigned headerSpace = sizeof(SlotDirectory) + (sd.numSlots * sizeof(Slot));

    //go through each slot and calculate space used by records
    void *slotData = (char*)pageData + sizeof(SlotDirectory);
    for (int i = 0; i < sd.numSlots; i++) {
        Slot slot;
        memcpy(&slot, slotData, sizeof(Slot));
        totalRecordSpace += slot.size; 
    }
    
    unsigned freeSpace = PAGE_SIZE - headerSpace - totalRecordSpace;
    
    return freeSpace;
}