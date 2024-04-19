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
    unsigned slotNum;
    SlotDirectory sd;
    unsigned freeSpace;
    for (int i = 0; i < fileHandle.getNumberOfPages(); i++) {
        
        sd = getSlotDirectory(pageData);
        freeSpace = getFreeSpace(pageData);

        fileHandle.readPage(i, pageData);
        
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
        sd = getSlotDirectory(new_pageData);
        freeSpace = getFreeSpace(new_pageData);
        pageNum = 0;
    }

    //update slot directory of the page we're using (temp)
    sd.FSO += totalDataSize;
    sd.numSlots += 1;

    memcpy(pageData, &sd, sizeof(SlotDirectory));

    //add new slot (or find first empty?)
    Slot newSlot;
    newSlot.RO = freeSpace + totalDataSize;
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
    
    return 0;
   
}   

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    //read entire page
    fileHandle.readPage(rid.pageNum, data);
    
    //goto correct slot
    Slot slot;
    void *slotData = (char*)data + sizeof(SlotDirectory) + (rid.slotNum * sizeof(Slot));
    memcpy(&slot, slotData, sizeof(Slot));


    cout << rid.slotNum;

    cout << slot.size << endl;
    cout << slot.RO;

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
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

unsigned RecordBasedFileManager::calculateRecordSize(const vector<Attribute> &recordDescriptor) {
    unsigned recordSize = 0;
    for (int i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        switch (attr.type)  {
            //only add bytes to recordSize if not NULL
            case TypeInt:
                //4 bytes
                recordSize += 4;
                break;
            case TypeReal:
                //4 bytes
                recordSize += 4;
                break;
            case TypeVarChar:
                //length + name (4 + length) bytes
                recordSize += 4 + attr.length;
        }
        return recordSize;
    }

}

SlotDirectory RecordBasedFileManager::getSlotDirectory(void *pageData) {
    SlotDirectory slot_directory;
    memcpy(&slot_directory, pageData, sizeof(SlotDirectory));
    return slot_directory;
}

void RecordBasedFileManager::configureSlotDirectory(SlotDirectory &sd, unsigned slots, unsigned offset) {
    sd.numSlots = slots;
    sd.FSO = offset;
}

unsigned RecordBasedFileManager::getFreeSpace(void *pageData) {
    SlotDirectory sd = getSlotDirectory(pageData); 
    unsigned occupied_space = sd.FSO + sizeof(SlotDirectory) + (sd.numSlots * sizeof(Slot));
    
    return PAGE_SIZE - occupied_space;
}