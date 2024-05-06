#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager::RecordBasedFileManager() {
    // Constructor implementation
}

RecordBasedFileManager::~RecordBasedFileManager() {
    // Destructor implementation
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return closeFile(fileHandle);
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting up the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
        return -1;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if(recordEntry.offset == 0)
        return -1;

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}


  RC RecordBasedFileManager::deleteRecord (FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;
    
    // Checking if slot id exisits
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
        return -1;

    // gets the slot record 
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    
        if(recordEntry.offset == 0)
        return -1;

    // copying the data that will overrite the deleted record
    // Check if pointer points at the head or tail of the record
    void * destination = (char *)pageData + slotHeader.freeSpaceOffset + recordEntry.length;
    void * source = (char *)pageData + slotHeader.freeSpaceOffset;
    size_t nbytes = recordEntry.offset - slotHeader.freeSpaceOffset;
    memcpy(destination, source, nbytes);  

    // updates all address that occur before the deleted record
    // determined by offset/address
    for(int i = 0; i < slotHeader.recordEntriesNumber; i++){
        SlotDirectoryRecordEntry record = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
        if(record.offset < recordEntry.offset){
            record.offset = record.offset + recordEntry.length;
            // void * recordAddress = (char *)pageData + sizeof(SlotDirectory) + (i * sizeof(SlotDirectoryRecordEntry) - 1);
            // memcpy()
            setSlotDirectoryRecordEntry(pageData, i, record);
        }
        

    }

    // updating slot
    recordEntry.length = 0;
    recordEntry.offset = 0;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

    // updating slot header
    slotHeader.freeSpaceOffset = slotHeader.freeSpaceOffset + recordEntry.length;
    setSlotDirectoryHeader(pageData, slotHeader);

    fileHandle.writePage(rid.pageNum, pageData);


    free(pageData);

    return SUCCESS;

  
    
}


// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    // Gets the size of the record
    unsigned newRecordSize = getRecordSize(recordDescriptor, data);
    
    SlotDirectoryRecordEntry forwardedRecordEntry;

    void * pageData = malloc(PAGE_SIZE);
    void * forwardPageData = malloc(PAGE_SIZE);

    if (fileHandle.readPage(rid.pageNum, pageData)) return RBFM_READ_FAILED;

    // Checking if slot id exisits
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber <= rid.slotNum) return RBFM_SLOT_DN_EXIST;

    // gets the slot record 
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    unsigned OGRecordSize = recordEntry.length;

    // Check if recordEntry is valid -- EXIT IF IS RID LENGTH AND OFFSET ARE BOTH 0 OR 0 AND -1
    if (recordEntry.length == 0 && (recordEntry.offset == 0 || recordEntry.offset == -1)) return RBFM_SLOT_DN_EXIST;

    // Calculate how much free space there is ON the page
    bool onPage = getPageFreeSpaceSize(pageData) >= newRecordSize;

    // Flag if recordEntry has already been forwarded (if offset & length are both negative)
    bool isForward = (recordEntry.length < 0) && (recordEntry.offset < 0);
    
    // TODO: Make switch statements instead of if-else ladder
    
    // Case 1: if recordSize == newRecordSize, overwrite the record with the new data
    if (newRecordSize == OGRecordSize) {
        // copying the data that will overrite the deleted record
        void * dataAddress = (char *)pageData + recordEntry.offset;
        memcpy(dataAddress, data, newRecordSize);

        // write the updated page back to the file
        if (fileHandle.writePage(rid.pageNum, pageData))
            return RBFM_WRITE_FAILED;
    }
    // Case 2: if newRecordSize < recordSize
    if (newRecordSize < OGRecordSize) {
        // overwrite old record size and update slot offset, slot length
        deleteRecord(fileHandle, recordDescriptor, rid); // delete record
        insertRecordOnPage(fileHandle, recordDescriptor, data, pageData, rid); // insert the smaller record on the same page and update rid
    }
    // Case 3: if newRecordSize > recordSize
    if (newRecordSize > OGRecordSize) {
        // Case 3.1 -- there is space on the page    
        if (onPage) {
            deleteRecord(fileHandle, recordDescriptor, rid); // delete record
            insertRecordOnPage(fileHandle, recordDescriptor, data, pageData, rid); // insert the larger record on the same page and update rid
        } else { 
            // Case 3.2 -- it doesn't fit on the same page
            deleteRecord(fileHandle, recordDescriptor, rid); // delete record
            RID newRid; // initialize empty rid for new insert record rid
            insertRecord(fileHandle, recordDescriptor, data, newRid);// call insert record
            
            // if page already forwarded, go to the forwarded record and change its len and offset to 0 and -1 to flag that record has been forwarded 
                
            if (isForward) {
                // call deleteRecord on the forwarded page
                // go to the forwarded rid to change it
                RID forwardedRid; 
                forwardedRid.pageNum = recordEntry.length*-1; 
                forwardedRid.slotNum = recordEntry.offset*-1;

                // read the page + get page entry
                if (fileHandle.readPage(forwardedRid.pageNum, forwardPageData)) return RBFM_READ_FAILED;
                forwardedRecordEntry = getSlotDirectoryRecordEntry(forwardPageData, forwardedRid.slotNum);
                
                deleteRecord(fileHandle, recordDescriptor, forwardedRid); // delete forwarded record

                // flag the previously forwarded record to 0 and -1 to show that it is no longer available!
                forwardedRecordEntry.length = 0;
                forwardedRecordEntry.offset = -1;

            }
            // Set the OG recordEntry's length and offset as both negative to "flag" that the record has been forwarded
            recordEntry.length = newRid.pageNum * -1;
            recordEntry.offset = newRid.slotNum * -1;
        }       
    }
            
    free(pageData);
    free(forwardPageData);
    return SUCCESS;
}


RC RecordBasedFileManager::insertRecordOnPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, void *pageData, const RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    setSlotDirectoryHeader(pageData, slotHeader); 

    // Adding the record data.
    setRecordAtOffset(pageData, newRecordEntry.offset, recordDescriptor, data);

    return SUCCESS;
}




SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;

    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i) {
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) {
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}


// Given a record descriptor, read a specific attribute of a record identified by a given rid.
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    void *recordData = malloc(PAGE_SIZE);
    readRecord(fileHandle, recordDescriptor, rid, recordData);
    
    int offset = 0;

    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*)recordData + offset, nullIndicatorSize);
    offset += nullIndicatorSize;

    int stringLength;
    bool found = false;

    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        Attribute attr = recordDescriptor[i];
        if (attr.name.compare(attributeName) == 0) {
            found = true;
        }
        switch (attr.type) {
            case TypeInt:
                if (found) {
                    memcpy(data, (char*)recordData + offset, INT_SIZE);
                    return 0;
                }

                offset += INT_SIZE;
                break;
            
            case TypeReal:
                if (found) {
                    memcpy(data, (char*)recordData + offset, REAL_SIZE);
                    return 0;
                }
               
                offset += REAL_SIZE;
                break;
            
            case TypeVarChar:
                memcpy(&stringLength, (char*)recordData + offset, VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                if (found) {
                    memcpy(data, (char*)recordData + offset, stringLength);
                    ((char*)data)[stringLength] = '\0';
                    return 0;
                }
                
                offset += stringLength;
                break;
        }
    }

    if (!found) {
        memcpy(data, "NULL", 4);
    }

    free(recordData);
    return -1;
    
}

/* ---------------------------------------- SCAN ------------------------------------------------- */
//  RecordBasedFileManager* RBFM_ScanIterator::_rbfmHelper = 0;
//RBFM_ScanIterator Class

// RBFM_ScanIterator* RBFM_ScanIterator::instance()
// {
//     if(!_rbfm_iterator)
//         _rbfm_iterator= new RBFM_ScanIterator();

//     return _rbfm_iterator;
// }
// RBFM_ScanIterator::RBFM_ScanIterator() 
// {}

// RBFM_ScanIterator::~RBFM_ScanIterator()
// {}

// initialize vars for rbfm scan
CompOp comp;
vector<Attribute> RD;
string condAttr;
vector<string> attrNames;
void* val;
RID iterRid;
unsigned numPages;
unsigned totalSlots;
FileHandle fileHandle;


// Scan returns an iterator to allow the caller to go through the results one by one. 
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    // incorrect syntax but she's got the spirit!
    // RBFM_ScanIterator rbfm_ScanIterator();
    // rbfm_ScanIterator.numPages = fileHandle.getNumberOfPages(); 

    fileHandle = fileHandle;
    comp = compOp;
    RD = recordDescriptor;
    condAttr = conditionAttribute;
    val = const_cast<void*>(value);
    attrNames = attributeNames;
    iterRid.pageNum = 0;
    iterRid.slotNum = 0;
    numPages = fileHandle.getNumberOfPages(); 
    totalSlots = 0;
    
    return 0;
}

bool RBFM_ScanIterator::isValid(SlotDirectoryRecordEntry recordEntry) {
    return recordEntry.length > 0 && recordEntry.offset > 0;
}

bool RBFM_ScanIterator::checkCondition() {
    // given the conditions: condition Attribute, comparison operator, and value to be compared:
    // what we need to do: see if read attribute matches cond attr
    void* data;
    
    // read record at Rid into data
    if (RecordBasedFileManager::readAttribute(fileHandle, RD, iterRid, condAttr, data) < 0) return false;

    // return the result of comparison between the found condition attribute and the specified value
    return data comp val;    
}

RC RBFM_ScanIterator::getNextSlot() {
    int RC = SUCCESS;
    SlotDirectoryHeader slotHeader;
    SlotDirectoryRecordEntry recordEntry;      
    void *pageData = malloc(PAGE_SIZE); // this could segfault if its recursive


    // increment iterator, handle case where you reach end of page and need new page
    iterRid.slotNum++;
    if (iterRid.slotNum >= totalSlots && (iterRid.pageNum < numPages)) {
        iterRid.pageNum++; // read new page
        iterRid.slotNum = 0;
        if (pageData == NULL) return RBFM_MALLOC_FAILED;

        // read in pageData
        fileHandle.readPage(iterRid.pageNum, pageData);
        slotHeader = getSlotDirectoryHeader(pageData);
        recordEntry = getSlotDirectoryRecordEntry(pageData, iterRid.slotNum);
        totalSlots = slotHeader.recordEntriesNumber;
        if (!isValid(recordEntry) || !checkCondition()) {
            return getNextSlot();
        }
    }
    else if (iterRid.pageNum >= numPages) RC = -1;

    free(pageData);
    return RC;
}

// Never keep the results in the memory. When getNextRecord() is called, 
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 
    int rc = getNextSlot(); // now the slot is in iterRid
    if (rc) return rc;
    
    void *pageData = malloc(PAGE_SIZE);
    
    // read in pageData
    fileHandle.readPage(iterRid.pageNum, pageData);
    SlotDirectoryRecordEntry recordEntry = rbfmInstance->getSlotDirectoryRecordEntryExternal(pageData, iterRid.slotNum);
    
    const void *OGdata = malloc(recordEntry.length);
    getRecordAtOffset(pageData, recordEntry.offset, RD, OGdata);

    unsigned OGRecordSize = getRecordSizeExternal(RD, OGdata);

    // Read in the null indicator from iterRid
    unsigned OGNullIndicatorSize = getNullIndicatorSizeExternal(RD.size());
    char* nullIndicator = (char*)malloc(OGNullIndicatorSize);
    memcpy(nullIndicator, OGdata, OGNullIndicatorSize);

    // setting up new null field indicator
    unsigned char* newNullIndicator[OGNullIndicatorSize];
    
    // filter the *data so that the null indicators are only pointing at the projected Attributes in "attrNames"
    // for every name in record descriptor, loop through attrNames to see if its a matching name
    for (unsigned i = 0; i < RD.size(); i++)
    {
        // if the existing nullIndicator is NULL at this field, set new nullIndicator to 0
        if (fieldIsNullExternal(nullIndicator, i)) {
            // set new null indicator to 0
            newNullIndicator[i] = 0;
            continue;
        }
        // loop through attrNames vector
        for (auto name : attrNames) {
            // if projected attribute is found in the recordDescriptor, set null indicator to 1
            if (name == RD[i].name) {
                // set null indicator to 1
                newNullIndicator[i] = 1;
            } else {
                // set null indicator to 0
                newNullIndicator[i] = 0;
            }        
        }
    }
    // memcpy record at iterRid but overwrite existing null indicators with new null indicators that only point to the projected attributes
    // 1) store OG data into data
    memcpy(data, OGdata, OGRecordSize);
    // 2) overwrite nullindicator bytes from data to have new null indicator bytes
    memcpy(data, newNullIndicator, OGNullIndicatorSize);
        // oh wait do i have to anticipate amt of entries is the first byte and then null indicators after? in that case i'd just add it to src or size

    return SUCCESS; 
}


RC RBFM_ScanIterator::close() { 
    // reset iterator to page 1 slot 0 (hypothetically)
    iterRid.pageNum = 0; iterRid.slotNum = 0;
    return SUCCESS;
}


