#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager *RecordBasedFileManager::_pf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
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
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

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

//Project 2 stuff

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void *pageData = malloc(PAGE_SIZE);
    
    fileHandle.readPage(rid.pageNum, pageData);

    SlotDirectoryHeader slotDirectory = getSlotDirectoryHeader(pageData);
    if (slotDirectory.recordEntriesNumber <= rid.slotNum) {
        //don't need to search if slot doesn't exist
        return -1;
    }

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector <Attribute> &recordDescriptor, const void *data, const RID &rid) {

    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector <Attribute> &recordDescriptor, const RID &rid,
const string &attributeName, void *data) 
{

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

    for (int i = 0; i < recordDescriptor.size(); i++) {
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

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.fileHandle = fileHandle;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.RD = recordDescriptor;
    rbfm_ScanIterator.condAttr = conditionAttribute;
    rbfm_ScanIterator.val = value;
    rbfm_ScanIterator.attrNames = attributeNames;
    rbfm_ScanIterator.iterRid.pageNum = 0;
    rbfm_ScanIterator.iterRid.slotNum = 0;
    rbfm_ScanIterator.currPageNum = fileHandle.getNumberOfPages(); 
    rbfm_ScanIterator.totalSlots = 0;

    return 0;
}


//Helper Functions

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

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
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


RBFM_ScanIterator* RBFM_ScanIterator::instance()
{
    if(!_rbf_iterator)
        _rbf_iterator = new RBFM_ScanIterator;

    return _rbf_iterator;
}
//RBFM_ScanIterator Class
RecordBasedFileManager* RBFM_ScanIterator::_rbfm = 0;

RBFM_ScanIterator::RBFM_ScanIterator() 
{
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
}

RC RBFM_ScanIterator::getNextValidSlot(RID &rid) {
    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, pageData);

    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, pageData, sizeof(SlotDirectoryHeader));

    //do we need to go to next page?
    if (rid.slotNum > slotHeader.recordEntriesNumber) {
        rid.pageNum += 1;
        rid.slotNum = 0;
    }

    //find the next valid slot
    SlotDirectoryRecordEntry recordEntry;

    memcpy  (
        &recordEntry,
        ((char*)pageData + sizeof(SlotDirectoryHeader) + rid.slotNum * sizeof(SlotDirectoryRecordEntry)),
        sizeof(SlotDirectoryRecordEntry)
    );
    
    if (recordEntry.length > 0 && recordEntry.offset > 0) {
        //we've found the next valid slot
        return SUCCESS;
    }
    
    rid.slotNum += 1;

    return -1;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 

    void *pageData = malloc(4096);
    fileHandle.readPage(currPageNum, pageData);

    //get next valid slot first
    while (getNextValidSlot(rid) != SUCCESS);

    //rid is valid -> readRecord at rid
    _rbfm->readRecord(fileHandle, RD, rid, data);

    //check each attribute
    void *temp;
    _rbfm->eadAttribute(fileHandle, RD, rid, condAttr, temp);

    //checkCondition
    if (!checkCondition(condAttr, RD, temp, compOp, val)) {
        return -1;
    }

    return 0;
    
}
  
RC RBFM_ScanIterator::close() { 
    return -1; 
}


bool RBFM_ScanIterator::checkCondition(const string &condAttr, const vector<Attribute> &recordDescriptor, void *temp, CompOp compOp, const void *value) {
    int tempInteger;
    float tempFloat;
    char *tempString;

    int valueInteger;
    float valueFloat;
    char *valueString;

    //use recordDescriptor to check what attrtype the condition attribute is
    AttrType attrType;
    for (const auto& attr : recordDescriptor) {
        if (attr.name.compare(condAttr) == 0) {
            attrType = attr.type;
        }
    }

    switch (attrType) {
        case TypeInt:
            memcpy(&tempInteger, temp, INT_SIZE);
            memcpy(&valueInteger, value, INT_SIZE);
            break;
    
        case TypeReal:
            memcpy(&tempFloat, temp, REAL_SIZE);
            memcpy(&valueFloat, value, REAL_SIZE);
            break;
        
        case TypeVarChar:
            tempString = (char*)malloc(51);
            memcpy(tempString, temp, 51);
            
            int stringLength = strlen(tempString);
            
            valueString = (char*)malloc(51);
            memcpy(valueString, value, 50);
            valueString[stringLength] = '\0';
    }
    
    
    bool condition = false;
    switch (compOp)
	{
		case EQ_OP:  
            if (attrType == TypeInt)
                condition = (tempInteger == valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat == valueFloat);
            else   
                condition = (strcmp(tempString, valueString) == 0);
            break;
		
        case LT_OP:  
            if (attrType == TypeInt)
                condition = (tempInteger < valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat < valueFloat);
            else   
                condition = (strcmp(tempString, valueString) < 0);
            break;
		
        case GT_OP:  
			if (attrType == TypeInt)
                condition = (tempInteger > valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat > valueFloat);
            else   
                condition = (strcmp(tempString, valueString) > 0);
            break;
	
        case LE_OP:  
			if (attrType == TypeInt)
                condition = (tempInteger <= valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat <= valueFloat);
            else   
                condition = (strcmp(tempString, valueString) <= 0);
            break;
		
        case GE_OP:  
			if (attrType == TypeInt)
                condition = (tempInteger >= valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat >= valueFloat);
            else   
                condition = (strcmp(tempString, valueString) >= 0);
            break;
		
        case NE_OP:  
			if (attrType == TypeInt)
                condition = (tempInteger != valueInteger);
            else if (attrType == TypeReal)
                condition = (tempFloat != valueFloat);
            else   
                condition = (strcmp(tempString, valueString) != 0);
            break;
		
        case NO_OP: 
			condition = true;
		    break;
	}

    if (valueString != NULL && tempString != NULL) {
        free(valueString);
        free(tempString);
    }

    return condition;
}