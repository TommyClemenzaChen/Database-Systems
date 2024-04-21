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

    unsigned recordSize = calculateRecordSize(recordDescriptor, data);
    unsigned totalDataSize = recordSize + sizeof(Slot);
    
    bool pageFound = false;
    unsigned pageNum;

    SlotDirectory sd;
    unsigned freeSpace;
    for (int i = 0; i < fileHandle.getNumberOfPages(); i++) {
        
        fileHandle.readPage(i, pageData);
        
        sd = getSlotDirectory(pageData);
        freeSpace = getFreeSpace(pageData);
        
        // changed
        if (totalDataSize <= freeSpace) {
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
    }

    //update slot directory of the page we're using
    sd.FSO -= recordSize;

    memcpy(pageData, &sd, sizeof(SlotDirectory));

    //add new slot (or find first empty?)
    Slot newSlot;
    newSlot.RO = sd.FSO;
    newSlot.size = recordSize;

    //find slot data position and insert
    void* slotData = (char*)pageData + sizeof(SlotDirectory) + (sd.numSlots * sizeof(Slot));

    memcpy(slotData, &newSlot, sizeof(Slot));

    sd.numSlots += 1;

    //find start of next record position and insert
    void* recordData = (char*)pageData + newSlot.RO;
    memcpy(recordData, data, recordSize);

    //set RID
    rid.pageNum = pageNum;
    rid.slotNum = sd.numSlots-1;
    

    fileHandle.writePage(pageNum, pageData);



    free(pageData);
    
    return 0;

}   

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    //read entire page
    void *pageData = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum, pageData) == -1){
        return -1;
    }

    

    //goto correct slot
    Slot slot = getSlot(pageData, rid.slotNum);


    void *recordData = (char*)pageData + slot.RO;
    
    memcpy(data, recordData, slot.size);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    unsigned first_n_bytes = (unsigned)ceil(recordDescriptor.size() / 8.0);

    unsigned nullFieldValue;
    memcpy(&nullFieldValue, data, first_n_bytes);
    
    //Attribute values
    int typeIntData;
    float typeRealData;
    
    unsigned length;
    char* typeStringData;
    
    unsigned bytes_read = first_n_bytes;

    for (int i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        if (!isNullField(nullFieldValue, i)) {
            switch (attr.type) {
                case TypeInt:
                    memcpy(&typeIntData, (char*)data + bytes_read, 4);
                    bytes_read += 4;
                    cout << attr.name << ": " << typeIntData << endl;
                    break;
                case TypeReal:
                    memcpy(&typeRealData, (char*)data + bytes_read, 4);
                    bytes_read += 4;
                    cout << attr.name << ": " << typeRealData << endl;
                    break;
                case TypeVarChar:
                    memcpy(&length, (char*)data + bytes_read, 4);
                    bytes_read += 4;

                    typeStringData = (char*)malloc(length+1);
                    memcpy(typeStringData, (char*)data + bytes_read, length);
                    bytes_read += length;
                   

                    typeStringData[length] = '\0';
                    cout << attr.name << ": " << typeStringData << endl;
                    free(typeStringData);

                    break;
            }
        }
        else {
            cout << attr.name << ": NULL";
        }
    }
    
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

bool RecordBasedFileManager::isNullField(unsigned nullFieldValue, int i) {
    unsigned check = (1 << (8-i)) & nullFieldValue;
    if (check == (1 << (8-i))) {
        return true;
    }
    return false;
}

unsigned RecordBasedFileManager::calculateRecordSize(const vector<Attribute> &recordDescriptor, const void* data) {
    unsigned first_n_bytes = (unsigned)ceil(recordDescriptor.size() / 8.0);

    vector<unsigned> nullFieldBytes(first_n_bytes);
    memcpy(nullFieldBytes.data(), data, first_n_bytes);

    unsigned bytes_read = first_n_bytes;
    unsigned length;
    for (int i = 0; i < recordDescriptor.size(); i++) {
        if (i != 0 && i % 7 == 0) {
            nullFieldBytes.erase(nullFieldBytes.begin());
        }
        Attribute attr = recordDescriptor[i];
        if(!isNullField(nullFieldBytes[0], i)) {
            switch (attr.type)  {
            //only add bytes to recordSize if not NULL
                case TypeInt:
                    //4 bytes
                    bytes_read += 4;
                    break;
                case TypeReal:
                    //4 bytes
                    bytes_read += 4;
                    break;
                case TypeVarChar:
                    //get the actual length of the varchar
                    memcpy(&length, (char*)data + bytes_read, 4);
                    bytes_read += 4 + length;
            }
        }
    }
    return bytes_read;
}


SlotDirectory RecordBasedFileManager::getSlotDirectory(void *pageData) {
    SlotDirectory slot_directory;
    memcpy(&slot_directory, pageData, sizeof(SlotDirectory));
    return slot_directory;
}

Slot RecordBasedFileManager::getSlot(const void *pageData, int slotNum) {
    // returns the slot
    
    Slot slot;
    void *slotAddress = (char*)pageData + sizeof(SlotDirectory) + (slotNum * sizeof(Slot));
    memcpy(&slot, slotAddress, sizeof(Slot));

    return slot;
}

unsigned RecordBasedFileManager::getFreeSpace(void *pageData) {
    SlotDirectory sd = getSlotDirectory(pageData);
    
    //HEADER = [SD][slot 1][slot 2][slot 3]...[slot n]
    unsigned headerSpace = sizeof(SlotDirectory) + (sd.numSlots * sizeof(Slot));
    
    unsigned freeSpace = sd.FSO - headerSpace;
    
    return freeSpace;
}