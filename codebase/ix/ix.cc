#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

//Helper Functions
bool IndexManager::fileExists(const string &fileName) {
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
}

unsigned IndexManager::getKeyStringLength(void *data) {
    unsigned stringLength;
    memcpy(&stringLength, data, sizeof(int));
    return stringLength;
}

RC IndexManager::newMetaPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write meta page header
    MetaPageHeader metaPageHeader;
    metaPageHeader.rootNum = 1;
    setMetaPageHeader(page, metaPageHeader);
    return SUCCESS;
}

RC IndexManager::newInternalPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write page header
    InternalPageHeader internalPageHeader;
    internalPageHeader.flag = INTERNAL;
    internalPageHeader.FSO = sizeof(InternalPageHeader);
    internalPageHeader.numEntries = 0;
    setInternalPageHeader(page, internalPageHeader);
    return SUCCESS;
}

RC IndexManager::newLeafPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write leaf page header
    LeafPageHeader leafPageHeader;
    leafPageHeader.flag = LEAF;
    leafPageHeader.FSO = sizeof(LeafPageHeader);
    leafPageHeader.numEntries = 0;
    setLeafPageHeader(page, leafPageHeader);
    return SUCCESS;
}

void IndexManager::setInternalPageHeader(void *page, InternalPageHeader internalPageHeader) {
    memcpy(page, &internalPageHeader, sizeof(InternalPageHeader));
}

InternalPageHeader IndexManager::getInternalPageHeader(void *page) {
    InternalPageHeader internalPageHeader;
    memcpy(&internalPageHeader, page, sizeof(InternalPageHeader));
    return internalPageHeader;
}

void IndexManager::setMetaPageHeader(void *page, MetaPageHeader metaPageHeader) {
    memcpy(page, &metaPageHeader, sizeof(MetaPageHeader));
}

MetaPageHeader IndexManager::getMetaPageHeader(void *page) {
    MetaPageHeader metaPageHeader;
    memcpy(&metaPageHeader, page, sizeof(MetaPageHeader));
    return metaPageHeader;
}

void IndexManager::setLeafPageHeader(void *page, LeafPageHeader leafPageHeader) {
    memcpy(page, &leafPageHeader, sizeof(LeafPageHeader));
}

LeafPageHeader IndexManager::getLeafPageHeader(void *page) {
    LeafPageHeader leafPageHeader;
    memcpy(&leafPageHeader, page, sizeof(LeafPageHeader));
    return leafPageHeader;
}

unsigned IndexManager::getRootPageNum(IXFileHandle ixfileHandle) {
    void *temp = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, temp);

    PageNum rootPageNum;
    memcpy(&rootPageNum, temp, sizeof(PageNum));

    free(temp);
    return rootPageNum;
}

unsigned IndexManager::getChildPageNum(const void *key, void *pageData, Attribute attr) {
    unsigned childPageNum;
    InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);

    unsigned keyLength = getKeyLength(key, attr);

    //keep reading until we find the correct child
    unsigned offset = sizeof(InternalPageHeader) + keyLength;
    
    for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
        memcpy(&childPageNum, (char*)pageData + offset, sizeof(PageNum));
        
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            return childPageNum;
        } //we need to find where key is smaller

        offset += sizeof(PageNum) + getKeyLength((char*)pageData + offset, attr);
    }

    return childPageNum;
}

bool IndexManager::isLeafPage(void *page) {
    LeafPageHeader leafPageHeader = getLeafPageHeader(page);
    return leafPageHeader.flag == LEAF;
}

bool IndexManager::isInternalPage(void *page) {
    InternalPageHeader internalPageHeader = getInternalPageHeader(page);
    return internalPageHeader.flag == INTERNAL;
}

unsigned IndexManager::getKeyLength(const void *key, const Attribute attr) {
    unsigned keyLength = 0;
    switch (attr.type) {
        
        case TypeInt:
            keyLength = sizeof(int);
            break;
        
        case TypeReal:
            keyLength = sizeof(float);
            break;
        
        case TypeVarChar:
            int stringLength;
            memcpy(&stringLength, key, sizeof(int));
            keyLength = sizeof(int) + stringLength;
            break;
    }

    return keyLength;
}

RC IndexManager::splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr, TrafficPair &trafficPair) {
    LeafPageHeader currLeafPageHeader = getLeafPageHeader(currLeafData); 
    
    unsigned currNumEntries = 0;
    unsigned offset = sizeof(LeafPageHeader);

    while (true) {
        if (offset >= PAGE_SIZE / 2)  //stop reading after 2048 bytes
            break;
        offset += getKeyLength((char*)currLeafData+offset, attr) + sizeof(RID); 
        currNumEntries += 1;
    }
    
    void *newLeafData = malloc(PAGE_SIZE);
    newLeafPage(newLeafData);
    LeafPageHeader newLeafPageHeader = getLeafPageHeader(newLeafData);
    newLeafPageHeader.numEntries = currLeafPageHeader.numEntries - currNumEntries;
    newLeafPageHeader.next = UINT_MAX;  //some number that we can't reach
    newLeafPageHeader.prev = currPageNum;
    setLeafPageHeader(newLeafData, newLeafPageHeader);

    // Split all the data that comes after offset into newLeafData
    memcpy((char*)newLeafData + newLeafPageHeader.FSO, (char*)currLeafData + offset, PAGE_SIZE - offset);
    
    ixFileHandle.appendPage(newLeafData);
    
    // Update the current leaf page to remove everything that was split from the page
    currLeafPageHeader.numEntries = currNumEntries; 
    currLeafPageHeader.FSO = offset; 
    currLeafPageHeader.next = ixFileHandle.getNumberOfPages();
    setLeafPageHeader(currLeafData, currLeafPageHeader);
    
    // Get middle entry and pass it as a traffic cop
    unsigned keyLength = getKeyLength((char*)currLeafData+offset, attr);
    
    void *middleKey = malloc(keyLength + sizeof(PageNum));
    
    memcpy(middleKey, (char*)currLeafData + offset, keyLength);
    
    // remove everything after offset from currLeafData
    memset((char*)currLeafData + offset, 0, PAGE_SIZE - offset);    

    ixFileHandle.writePage(currPageNum, currLeafData);
    
    trafficPair.key = middleKey;
    trafficPair.pageNum = ixFileHandle.getNumberOfPages(); //this is pageNum of newLeafData (split page)

    free(newLeafData);
    return SUCCESS;
}

RC IndexManager::splitInternalPage(void * currInternalData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr, TrafficPair &trafficPair) {
    InternalPageHeader currInternalPageHeader = getInternalPageHeader(currInternalData); 
    
    unsigned currNumEntries = 0;
    unsigned offset = sizeof(InternalPageHeader);

    while (true) {
        if (offset >= PAGE_SIZE / 2)  //stop reading after 2048 bytes
            break;
        offset += getKeyLength((char*)currInternalData+offset, attr) + sizeof(PageNum); 
        currNumEntries++;
    }

    // Get middle entry and pass it as a traffic cop
    unsigned keyLength = getKeyLength((char*)currInternalData+offset, attr);
    void *middleKey = malloc(keyLength + sizeof(PageNum));
    
    memcpy(middleKey, (char*)currInternalData + offset, keyLength);
    
    void *newInternalData = malloc(PAGE_SIZE);
    newInternalPage(newInternalData);
    InternalPageHeader newInternalPageHeader = getInternalPageHeader(newInternalData);
    newInternalPageHeader.numEntries = currInternalPageHeader.numEntries - currNumEntries - 1; // -1 so you subtract the middle key entry
    newInternalPageHeader.FSO = PAGE_SIZE - offset - keyLength - sizeof(PageNum);
    setInternalPageHeader(newInternalData, newInternalPageHeader);

    // Split all the data that comes after offset and middle key into newInternalData
    memcpy((char*)newInternalData + newInternalPageHeader.FSO, (char*)currInternalData + offset + keyLength + sizeof(PageNum), PAGE_SIZE - offset - keyLength - sizeof(PageNum));
    
    ixFileHandle.appendPage(newInternalData);
    
    // Update the current leaf page to remove everything that was split from the page
    currInternalPageHeader.numEntries = currNumEntries; 
    currInternalPageHeader.FSO = offset; 
    setInternalPageHeader(currInternalData, currInternalPageHeader);
        
    // remove everything after offset and middle key from currLeafData
    memset((char*)currInternalData+offset+keyLength+sizeof(PageNum), 0, PAGE_SIZE - offset); 

    ixFileHandle.writePage(currPageNum, currInternalData);

    trafficPair.key = middleKey;
    trafficPair.pageNum = ixFileHandle.getNumberOfPages(); //this is pageNum of newInternalData (split page)

    // PRINT THE FIRST KEY IN THE NEW INTERNAL PAGE
    unsigned newInternalKeySize = getKeyLength((char*)newInternalData + sizeof(InternalPageHeader), attr);
    cout << "[Test] New internal key size: " << newInternalKeySize << endl;
    char *temp = (char*)malloc(newInternalKeySize + 1);
    memcpy(temp, (char*)newInternalData + sizeof(InternalPageHeader)+sizeof(int), newInternalKeySize+sizeof(PageNum));
    temp[newInternalKeySize] = '\0';
    cout << "First key in new internal: " << temp << endl;

    cout << "Old internalPageHeader FSO: " << currInternalPageHeader.FSO << endl;
    cout << "Middle key length: " << keyLength + sizeof(PageNum) << endl;  
    cout << "New internalpageheader FSO: " << newInternalPageHeader.FSO << endl;

    return SUCCESS;
}

int IndexManager::compareKeys(Attribute attr, const void* key1, const void* key2) {
    int valueInt1;
    int valueInt2;

    float valueFloat1;
    float valueFloat2;

    int stringLength1;
    int stringLength2;
    char* str1;
    char* str2;


    switch (attr.type) {
        case TypeInt:
            
            memcpy(&valueInt1, key1, sizeof(int));
            
            memcpy(&valueInt2, key2, sizeof(int));
            
            if (valueInt1 < valueInt2)
                return -1;
            else if (valueInt1 > valueInt2)
                return 1;
            else
                return 0;

        case TypeReal:
            memcpy(&valueFloat1, key1, sizeof(float));
            
            memcpy(&valueFloat2, key2, sizeof(float));

            if (valueFloat1 < valueFloat2)
                return -1;
            else if (valueFloat1 > valueFloat2)
                return 1;
            else
                return 0;
        
        case TypeVarChar:
            
            memcpy(&stringLength1, key1, sizeof(int));
            str1 = (char*)malloc(stringLength1 + 1);
            memcpy(str1, (char*)key1 + sizeof(int), stringLength1);


            memcpy(&stringLength2, key2, sizeof(int));
            str2 = (char*)malloc(stringLength2 + 1);
            memcpy(str2, (char*)key2 + sizeof(int), stringLength2);
        
            int rc = strcmp(str1, str2);
            free(str1);
            free(str2);

            if (rc > 0)
                return 1;
            else if (rc < 0)
                return -1;
            else
                return 0;
            
    }
    return 0;
}

bool IndexManager::compareRIDS(const RID &rid1, const RID &rid2) {
    if (rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum) {
        return true;
    }

    return false;
}

bool IndexManager::leafPairExists(void *pageData, const void *key, const RID &rid, const Attribute &attr) {
    LeafPageHeader leafHeader = getLeafPageHeader(pageData);

    RID tempRID;
	unsigned offset = sizeof(LeafPageHeader);
    unsigned tempKeyLength;
    for (unsigned i = 0; i < leafHeader.numEntries; i++) {
		tempKeyLength = getKeyLength((char*) pageData + offset, attr);
		memcpy(&tempRID, (char*) pageData + offset + tempKeyLength, sizeof(RID));

		if(compareKeys(attr, key, (char*) pageData + offset) == 0 && compareRIDS(tempRID, rid))
			return true;

		offset += tempKeyLength + sizeof(RID);
	}

    return false;
}

bool IndexManager::trafficPairExists(void *pageData, const void *key, const PageNum pageNum, const Attribute &attr) {
    InternalPageHeader internalHeader = getInternalPageHeader(pageData);

    PageNum tempPageNum;
    unsigned offset = sizeof(InternalPageHeader);
    unsigned tempKeyLength;
    for (unsigned i = 0; i < internalHeader.numEntries; i++) {
        tempKeyLength = getKeyLength((char*)pageData + offset, attr);
        memcpy(&tempPageNum, (char*)pageData + offset + tempKeyLength, sizeof(PageNum));

        if (compareKeys(attr, key, (char*)pageData + offset) == 0 && tempPageNum == pageNum) {
            return true;
        }

        offset += tempKeyLength + sizeof(PageNum);
    }

    return false;
}

RC IndexManager::createFile(const string &fileName)
{
        if (fileExists(fileName)) {
        return 1;
    }
    
    FILE *iFile = fopen(fileName.c_str(), "wb");

    if (iFile == NULL) {
        return 2;
    }

    //start adding pages
    void *metaPageData = malloc(PAGE_SIZE);
    if (metaPageData == NULL) {
        return 3;
    }
    newMetaPage(metaPageData);
    

    void *rootPageData = malloc(PAGE_SIZE);
    if (rootPageData == NULL) {
        return 4;
    }
    //root is always internal, except we know it's root from meta info
    newInternalPage(rootPageData);

    void *leafPageData = malloc(PAGE_SIZE);
    if (leafPageData == NULL) {
        return 5;
    }

    newLeafPage(leafPageData);

    IXFileHandle ixfileHandle;
    ixfileHandle.setfd(iFile);


    ixfileHandle.appendPage(metaPageData);
    ixfileHandle.appendPage(rootPageData);
    ixfileHandle.appendPage(leafPageData);

    closeFile(ixfileHandle);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    if (remove(fileName.c_str()) != 0) {
        return -1;
    }

    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    //ixFileHandle already in use
    if (ixfileHandle.getfd() != NULL) {
        return 1;
    }

    //no file exists
    if (!fileExists(fileName)) {
        return 2;
    }

    FILE *iFile;
    iFile = fopen(fileName.c_str(), "rb+");
    if (iFile == NULL) {
        return 3;
    }

    ixfileHandle.setfd(iFile);

    return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FILE *pFile = ixfileHandle.getfd();

    // If not an open file, error
    if (pFile == NULL)
        return 1;

    // Flush and close the file
    fclose(pFile);

    ixfileHandle.setfd(NULL);

    return SUCCESS;
}

RC IndexManager::insertInternalPair(void *pageData, const Attribute &attr, const void *key, const PageNum pageNum) {
    //we don't want duplicates (should never get here)
    if (trafficPairExists(pageData, key, pageNum, attr)) {
        return DUPLICATE;
    }
    
    InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);
    //at this point, trafficPair shouldn't have null fields
    unsigned keyLength = getKeyLength(key, attr);
    
    if (PAGE_SIZE - internalPageHeader.FSO < keyLength + sizeof(PageNum)) {
        return NO_SPACE;
    }

    //find the correct index
    unsigned offset = sizeof(InternalPageHeader);
    for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            break;
        }
        offset += keyLength + sizeof(PageNum);
    }

    //for entires that come after, shift to the right
    memcpy((char*)pageData + keyLength + sizeof(PageNum), (char*)pageData + offset, internalPageHeader.FSO - offset);
    
    //insert the new trafficPair
    memcpy((char*)pageData + offset, key, keyLength);
    memcpy((char*)pageData + offset + keyLength, &pageNum, sizeof(PageNum));

    internalPageHeader.FSO -= keyLength + sizeof(PageNum);
    internalPageHeader.numEntries += 1;

    return SUCCESS;

    return 0;
}

RC IndexManager::insertLeafPair(void *pageData, const Attribute &attr, const void *key, const RID &rid) {
    //we don't want duplicates
    if (leafPairExists(pageData, key, rid, attr)) {
        return DUPLICATE;
    }

    LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
    unsigned keyLength = getKeyLength(key, attr);

    if (PAGE_SIZE - leafPageHeader.FSO < keyLength + sizeof(RID)) {
        return NO_SPACE;
    }
    
    unsigned offset = sizeof(LeafPageHeader);
    for (unsigned i = 0; i < leafPageHeader.numEntries; i++) {
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            break;
        }
        offset += keyLength + sizeof(RID);
    }
    
    cout << offset << endl;
    cout << leafPageHeader.FSO << endl;

    //for entires that come after, shift to the right
    memcpy((char*)pageData + keyLength + sizeof(RID), (char*)pageData + offset, leafPageHeader.FSO - offset);
    
    //insert the new entry
    memcpy((char*)pageData + offset, key, keyLength);
    memcpy((char*)pageData + offset + keyLength, &rid, sizeof(RID));

    leafPageHeader.FSO -= keyLength + sizeof(RID);
    leafPageHeader.numEntries += 1;

    return SUCCESS;
}

RC IndexManager::insert(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid, const unsigned pageNum, TrafficPair &trafficPair) {
    
    void *pageData = malloc(PAGE_SIZE);
    cout << "PageNum: " << pageNum << endl;
    //start from the root page 
    if (ixfileHandle.readPage(pageNum, pageData) != SUCCESS) {
        return 1;
    }

    //Internal Page
    RC rc;
    if (isInternalPage(pageData)) {
        unsigned childPageNum = getChildPageNum(key, pageData, attr);
        rc = insert(ixfileHandle, attr, key, rid, childPageNum, trafficPair);
        if (trafficPair.key == NULL) {
            free(pageData);
            return 0;
        }

        //since trafficPair does not have NULL key, we have split a leaf page
        rc = insertInternalPair(pageData, attr, trafficPair.key, trafficPair.pageNum);
        if (rc == DUPLICATE) {
            return DUPLICATE;
        }

        if (rc == NO_SPACE) {
            splitInternalPage(pageData, pageNum, ixfileHandle, attr, trafficPair); // empty traffic cop

            //try inserting again after splitting
            rc = insertInternalPair(pageData, attr, trafficPair.key, trafficPair.pageNum);
        }

        if (rc == SUCCESS) {
            ixfileHandle.writePage(pageNum, pageData);
            trafficPair.key = NULL;
        }
    }

    //Leaf Page
    else if (isLeafPage(pageData)) {
        //try inserting and find out if there's free space
        rc = insertLeafPair(pageData, attr, key, rid);
        if (rc == DUPLICATE) {
            return DUPLICATE;
        }

        if (rc == NO_SPACE) {
            splitLeafPage(pageData, pageNum, ixfileHandle, attr, trafficPair);
            //try inserting again after splitting
            rc = insertLeafPair(pageData, attr, key, rid);
        }

        if (rc == SUCCESS) {
            ixfileHandle.writePage(pageNum, pageData);
            trafficPair.key = NULL;
            free(pageData);
            return SUCCESS;
        }

        return -1;   
    }
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //we might need to split, so we pass in a NULL traffic cop
    TrafficPair trafficPair;
    trafficPair.key = NULL;
    trafficPair.pageNum = UINT_MAX;

    unsigned rootPageNum = getRootPageNum(ixfileHandle);

    //recursive calls until alll splits and inserts are complete
    return insert(ixfileHandle, attribute, key, rid, rootPageNum, trafficPair);

}


// when i wrote this code only god and i knew how it worked. now, only god knows.
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{/*
    unsigned j = getRootNum(ixfileHandle);
    
    void* data = malloc(PAGE_SIZE);
    
    void* internalKey;
    unsigned prevPageNum = UINT_MAX;
    
    while (true) {
        readPage(j, data); // read in the root page
        if (getFlag(data) == INTERNAL) {
            InternalPageHeader internalPageHeader = getInternalPageHeader(data);
            offset += sizeof(InternalPageHeader);
            while (true) {
                // get the key at the next entry
                memcpy(&internalKey, (char*)data+offset, getKeyLength(attribute, key));
                if (compareKeys(attribute, internalKey, key) <= 0) {
                    // since we found less than or equal to the key, we need to fetch the corresponding pageNum
                    unsigned keyPageNum;
                    memcpy(&keyPageNum, (char*)data+offset+getKeyLength(attribute, key), sizeof(int));
                    prevPageNum = keyPageNum; // set the prevPageNum to the one that's less than or equal to 
                    offset += getKeyLength(attribute, key) + sizeof(PageNum);
                }
                else {
                    // since the only entries in the page are greater than the key, the key must not exist
                    if (prevPageNum == UINT_MAX) {
                        return KEY_NOT_FOUND; }
                    // else, go back to the previous pageNum which has an internal key that is less than or equal to the key, and return that
                    j = prevPageNum;
                    break;
                }
            }
            offset = 0; // reset offset for next page to be read in if necessary
        }
        else { // is leaf page
            // we've found the actual page where the key is stored! yay!
            LeafPageHeader leafPageHeader = getLeafPageHeader(data);
            offset = sizeof(LeafPageHeader); 

            // index through all the entries in the page until you find a match
            while (true) {
                // get the key at the next entry
                memcpy(&internalKey, (char*)data+offset, getKeyLength(attribute, key));
                if (compareKeys(attribute, internalKey, key) == 0) {
                    // key found! yay! now we delete
                    unsigned entrySize = getKeyLength(attribute, key) + sizeof(RID);

                    // Move subsequent entries up to fill the gap
                    memmove((char*)data + offset, (char*)data + offset + entrySize, PAGE_SIZE - offset - entrySize);

                    // Update the leaf page header
                    leafPageHeader.numEntries--;
                    leafPageHeader.freeSpaceOffset -= entrySize;

                    // Write the updated header back to the data
                    memcpy(data, &leafPageHeader, sizeof(LeafPageHeader));

                    // Write the modified page back to the file
                    ixfileHandle.writePage(j, data);

                    free(internalKey); // Free allocated memory
                    free(data); // Free allocated memory
                    return SUCCESS;
                }
                else if (compareKeys(attribute, internalKey, key) > 0) {
                    free(internalKey); // Free allocated memory
                    free(data); // Free allocated memory
                    return KEY_NOT_FOUND; // could also use recordExists() at the beginning of the else if {}
                } else {
                    offset += getKeyLength(attribute, key) + sizeof(RID);
                }
            }
        }
    }
    free(internalKey); // Free allocated memory
    free(data); // Free allocated memory */
    return SUCCESS;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    cout << "not implemented yet" << endl;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    // free(pageData);
    return SUCCESS;
}

IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;

    _fd = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    // If pageNum doesn't exist, error
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Try to seek to the specified page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Try to read the specified page
    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return FH_READ_FAILED;

    ixReadPageCounter++;
    return SUCCESS;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    // Check if the page exists
    if (getNumberOfPages() < pageNum)
        return FH_PAGE_DN_EXIST;

    // Seek to the start of the page
    if (fseek(_fd, PAGE_SIZE * pageNum, SEEK_SET))
        return FH_SEEK_FAILED;

    // Write the page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        // Immediately commit changes to disk
        fflush(_fd);
        ixWritePageCounter++;
        return SUCCESS;
    }
    
    return FH_WRITE_FAILED;
}


RC IXFileHandle::appendPage(const void *data)
{
    // Seek to the end of the file
    if (fseek(_fd, 0, SEEK_END))
        return FH_SEEK_FAILED;

    // Write the new page
    if (fwrite(data, 1, PAGE_SIZE, _fd) == PAGE_SIZE)
    {
        fflush(_fd);
        ixAppendPageCounter++;
        return SUCCESS;
    }
    return FH_WRITE_FAILED;
}


unsigned IXFileHandle::getNumberOfPages()
{
    // Use stat to get the file size
    struct stat sb;
    if (fstat(fileno(_fd), &sb) != 0)
        // On error, return 0
        return 0;
    // Filesize is always PAGE_SIZE * number of pages
    return sb.st_size / PAGE_SIZE;
}


RC IXFileHandle::collectCounterValues(unsigned &ixReadPageCount, unsigned &ixWritePageCount, unsigned &ixAppendPageCount)
{
    ixReadPageCount = ixReadPageCounter;
    ixWritePageCount  = ixWritePageCounter;
    ixAppendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

void IXFileHandle::setfd(FILE *fd) {
    _fd = fd;
}

FILE* IXFileHandle::getfd() {
    return _fd;
}
