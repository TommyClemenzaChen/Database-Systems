
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

RC IndexManager::newInternalPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write page header
    InternalPageHeader internalPageHeader;
    internalPageHeader.flag = INTERNAL;
    internalPageHeader.FSO = PAGE_SIZE;
    internalPageHeader.numEntries = 1; //??
    setInternalPageHeader(page, internalPageHeader);
    return SUCCESS;
}

RC IndexManager::newMetaPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write meta page header
    MetaPageHeader metaPageHeader;
    metaPageHeader.rootNum = 1;
    setMetaPageHeader(page, metaPageHeader);
    return SUCCESS;
}

RC IndexManager::newLeafPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write leaf page header
    LeafPageHeader leafPageHeader;
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

bool IndexManager::isLeafPage(void *page) {
    LeafPageHeader leafPageHeader;
    memcpy(&leafPageHeader, page, sizeof(LeafPageHeader));
    
    return leafPageHeader.flag == LEAF;
}

bool IndexManager::isInternalPage(void *page) {
    InternalPageHeader internalPageHeader;
    memcpy(&internalPageHeader, page, sizeof(InternalPageHeader));
    return internalPageHeader.flag == INTERNAL;
}

unsigned getKeyLength(void *key, Attribute attr) {
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

RC IndexManager::splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr) {

    LeafPageHeader currLeafPageHeader = getLeafPageHeader(currLeafData); 
    
    RID rid;
    unsigned currNumEntries;
    unsigned offset = sizeof(LeafPageHeader);
    while (true) {
        if (offset >= PAGE_SIZE / 2)  //stop reading after 2048 bytes
            break;
        offset += sizeOfAttr(attr, (char*)currLeafData+offset, rid); 
        currNumEntries += 1;
    }

    void *newLeafData = malloc(PAGE_SIZE);
    LeafPageHeader newLeafPageHeader;
    newLeafPageHeader.flag = LEAF;
    newLeafPageHeader.numEntries = currLeafPageHeader.numEntries - currNumEntries; // get the remaining number of entries on the page
    newLeafPageHeader.FSO = PAGE_SIZE - offset;  // this may not be accurate, but if the page is full this should be true
    newLeafPageHeader.next = 0; // set to 0 bc you cant set an unsigned to NULL
    newLeafPageHeader.prev = currPageNum; 
    

    /* ------------------------------------
        What order should these 3 lines be in? */
    setLeafPageHeader(newLeafData, newLeafPageHeader);

    ixFileHandle.appendPage(newLeafData);

    // Split all the data that comes after offset into newInternalData
    memcpy(newLeafData, (char*)currLeafData + offset, PAGE_SIZE - offset);

    /* ------------------------------------*/

    // Update the current leaf page to remove everything that was split from the page
    currLeafPageHeader.numEntries = currNumEntries; 
    currLeafPageHeader.FSO = offset; 
    currLeafPageHeader.next = ixFileHandle.getNumberOfPages(); 
    // don't set currLeafPageHeader.prev because it may have been previously set

    // TODO: remove everything after offset from currLeafData
    memset((char*)currLeafData + offset, 0, PAGE_SIZE - offset);    

    return SUCCESS;
}

RC IndexManager::splitInternalPage(void * currInternalData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr) {
    InternalPageHeader currInternalPageHeader = getInternalPageHeader(currInternalData); 

    PageNum pagenum;    
    unsigned currNumEntries;
    unsigned offset = sizeof(InternalPageHeader);
    while (true) {
        if (offset >= PAGE_SIZE / 2)  //stop reading after 2048 bytes
            break;
        offset += sizeOfKey(attr, (char*)currInternalData+offset) + sizeof(PageNum);
        currNumEntries += 1;
    }

    void *newInternalData = malloc(PAGE_SIZE);
    InternalPageHeader newInternalPageHeader;
    newInternalPageHeader.flag = INTERNAL;
    newInternalPageHeader.numEntries = currInternalPageHeader.numEntries - currNumEntries; // get the remaining number of entries on the page
    newInternalPageHeader.FSO = PAGE_SIZE - offset;  // this may not be accurate, but if the page is full this should be true
    
    /* ------------------------------------
        What order should these 3 lines be in? */
    setInternalPageHeader(newInternalData, newInternalPageHeader);

    ixFileHandle.appendPage(newInternalData);

    // Split all the data that comes after offset into newInternalData
    memcpy(newInternalData, (char*)currInternalData + offset, PAGE_SIZE - offset);

    /* ------------------------------------*/

    // Update the current internal page to remove everything that was split from the page
    currInternalPageHeader.numEntries = currNumEntries; 
    currInternalPageHeader.FSO = offset; 

    // TODO: remove everything after offset from currPageData
    memset((char*)currInternalData + offset, 0, PAGE_SIZE - offset);    

    return SUCCESS;
}

// helper functions for insert
bool IndexManager::cantInsertInternal(InternalPageHeader internalPageHeader, Attribute attr, void *key, RID rid) {
    int newEntrySize = sizeOfAttr(attr, key, rid);
    return internalPageHeader.FSO - sizeof(InternalPageHeader) < newEntrySize;
}

int IndexManager::sizeOfAttr(Attribute attr, void* key, RID &rid) {
    return sizeOfKey(attr, key) + sizeof(rid);
}

int IndexManager::sizeOfKey(Attribute attr, void* key) {
    int varCharSize = 0;
    if (attr.type == TypeReal || attr.type == TypeInt)
        return 4;
    // read in the first 4 bytes of the key
    int stringLength;
    memcpy(&stringLength, (char*)key, sizeof(int)); // get the length of the string
    return 4 + stringLength;
}

int IndexManager::compareKeys(Attribute attr, void* key1, void* key2) {
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
}

bool IndexManager::compareRIDS(RID &rid1, RID &rid2) {
    if (rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum) {
        return true;
    }
    return false;
}

bool IndexManager::recordExists(void *pageData, void *key, RID &rid, Attribute &attr) {
    LeafPageHeader leafHeader = getLeafPageHeader(pageData);

    RID tempRID;
	unsigned offset = sizeof(LeafPageHeader);
    unsigned tempKeyLength;
    for (int i = 0; i < leafHeader.numEntries; i++) {
		tempKeyLength = getKeyLength((char*) pageData + offset, attr);
		memcpy(&tempRID, (char*) pageData + offset + tempKeyLength, sizeof(RID));

		// Compares the current read key and RID with the ones given in input.
		// If we have found the same <key, reid> pair, return true.
		if(compareKeys(attr, key, (char*) pageData + offset) == 0 && compareRIDS(tempRID, rid));
			return true;

		offset += tempKeyLength + sizeof(RID);
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

/*????
    void *childPageData = malloc(PAGE_SIZE);
    if (childPageData == NULL) {
        return 5;
    }
*/

    IXFileHandle ixfileHandle;
    ixfileHandle.setfd(iFile);

    ixfileHandle.appendPage(metaPageData);
    ixfileHandle.appendPage(rootPageData);

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

RC IndexManager::insert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    //Internal Page
    

    //Leaf Page
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    TrafficPair trafficPair;
    trafficPair.key = NULL;

    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

    return -1;
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
    return -1;
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

