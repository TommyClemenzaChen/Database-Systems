#include "ix.h"
// #include <format>
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

void IndexManager::printLeafPageHeader(LeafPageHeader leafPageHeader) {
    cout << leafPageHeader.flag << endl;
    cout << leafPageHeader.FSO << endl;
    cout << leafPageHeader.next << endl;
    cout << leafPageHeader.prev << endl;
    cout << leafPageHeader.numEntries << endl;
}

void IndexManager::printInternalPageHeader(InternalPageHeader internalPageHeader) {
    cout << internalPageHeader.flag << endl;
    cout << internalPageHeader.FSO << endl;
    cout << internalPageHeader.numEntries << endl;
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
    internalPageHeader.leftChildPage = UINT_MAX;
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
    leafPageHeader.next = UINT_MAX;
    leafPageHeader.prev = UINT_MAX;
    setLeafPageHeader(page, leafPageHeader);
    return SUCCESS;
}

void IndexManager::setInternalPageHeader(void *page, InternalPageHeader internalPageHeader) {
    memcpy(page, &internalPageHeader, sizeof(InternalPageHeader));
}

InternalPageHeader IndexManager::getInternalPageHeader(void *page) const {
    InternalPageHeader internalPageHeader;
    memcpy(&internalPageHeader, page, sizeof(InternalPageHeader));
    return internalPageHeader;
}

void IndexManager::setMetaPageHeader(void *page, MetaPageHeader metaPageHeader) {
    memcpy(page, &metaPageHeader, sizeof(MetaPageHeader));
}

MetaPageHeader IndexManager::getMetaPageHeader(void *page) const {
    MetaPageHeader metaPageHeader;
    memcpy(&metaPageHeader, page, sizeof(MetaPageHeader));
    return metaPageHeader;
}

void IndexManager::setLeafPageHeader(void *page, LeafPageHeader leafPageHeader) {
    memcpy(page, &leafPageHeader, sizeof(LeafPageHeader));
}

LeafPageHeader IndexManager::getLeafPageHeader(void *page) const {
    LeafPageHeader leafPageHeader;
    memcpy(&leafPageHeader, page, sizeof(LeafPageHeader));
    return leafPageHeader;
}

PageNum IndexManager::getRootPageNum(IXFileHandle &ixfileHandle) const {
    void *temp = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0, temp);

    PageNum rootPageNum;
    memcpy(&rootPageNum, temp, sizeof(PageNum));

    free(temp);
    return rootPageNum;
}

PageNum IndexManager::getChildPageNum(const void *key, void *pageData, Attribute attr) const {
    InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);
    unsigned childPageNum = internalPageHeader.leftChildPage;

    //keep reading until we find the correct child
    unsigned offset = sizeof(InternalPageHeader);
    unsigned keyLength = 0;

    for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            break;
        } 

        keyLength = getKeyLength(key, attr);
        memcpy(&childPageNum, (char*)pageData + offset + keyLength, sizeof(PageNum));
        
        offset += keyLength + sizeof(PageNum);
    }

    //correct pageNum is to the left of the last key we read
    if (internalPageHeader.numEntries > 0) {
        keyLength = getKeyLength((char*)pageData + offset, attr);
        memcpy(&childPageNum, (char*)pageData + offset + keyLength, sizeof(PageNum));
    }
    
    // cout << "childPage: " << childPageNum << endl;
    return childPageNum;
}


Flag IndexManager::getFlag(void *data) const {
    Flag flag;
    memcpy(&flag, data, sizeof(Flag));
    return flag;
}

unsigned IndexManager::getKeyLength(const void *key, const Attribute attr) const {
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

RC IndexManager::splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle &ixFileHandle, Attribute attr, TrafficPair &trafficPair) {
    LeafPageHeader currLeafPageHeader = getLeafPageHeader(currLeafData); 
    
    unsigned currNumEntries = 0;
    unsigned offset = sizeof(LeafPageHeader);

    while (true) {
        if (offset >= PAGE_SIZE / 2)  //stop reading after 2048 bytes
            break;
        offset += getKeyLength((char*)currLeafData+offset, attr) + sizeof(RID); 
        currNumEntries++;
    }

    // Get middle entry and pass it as a traffic cop
    unsigned keyLength = getKeyLength((char*)currLeafData+offset, attr);
    void *middleKey = malloc(keyLength);
    memcpy(middleKey, (char*)currLeafData + offset, keyLength);

    //calculate the chunk size of the split data
    unsigned splitDataSize = PAGE_SIZE - offset;

    // initialize new leaf page
    void *newLeafData = malloc(PAGE_SIZE);
    newLeafPage(newLeafData);
    LeafPageHeader newLeafPageHeader = getLeafPageHeader(newLeafData);
    newLeafPageHeader.FSO = sizeof(LeafPageHeader) + splitDataSize;
    newLeafPageHeader.numEntries = currLeafPageHeader.numEntries - currNumEntries;
    newLeafPageHeader.next = UINT_MAX; // some number that we can't reach
    newLeafPageHeader.prev = currPageNum;

    // loop to link leaves
    if (currLeafPageHeader.next != UINT_MAX) {
        void* linkPageData = malloc(PAGE_SIZE);
        unsigned linkPageNum = currPageNum;
        LeafPageHeader linkLeafPageHeader;
        while (linkLeafPageHeader.next != UINT_MAX) {
            linkPageNum = linkLeafPageHeader.next;
            ixFileHandle.readPage(linkPageNum, linkPageData);
            linkLeafPageHeader = getLeafPageHeader(linkPageData);
            // if (linkLeafPageHeader.next == UINT_MAX) break;
        }
        newLeafPageHeader.prev = linkPageNum;
        linkLeafPageHeader.next = ixFileHandle.getNumberOfPages();
        setLeafPageHeader(linkPageData, linkLeafPageHeader);
        ixFileHandle.writePage(linkPageNum, linkPageData);
    }
    else {
        currLeafPageHeader.next = ixFileHandle.getNumberOfPages();
    }
    
    // now that you've adjusted
    setLeafPageHeader(newLeafData, newLeafPageHeader);

    // Split all the data that comes after offset into newLeafData
    memcpy((char*)newLeafData + sizeof(LeafPageHeader), (char*)currLeafData + offset, splitDataSize);
    
    cout << "Splitting page. Curr PageNum: " << currPageNum << ", Prev total num pages: " << ixFileHandle.getNumberOfPages();

    ixFileHandle.appendPage(newLeafData);

    cout << ", New total num pages: " << ixFileHandle.getNumberOfPages() << endl;
    
    // Update the current leaf page to remove everything that was split from the page
    currLeafPageHeader.numEntries = currNumEntries; 
    currLeafPageHeader.FSO = offset; 
    setLeafPageHeader(currLeafData, currLeafPageHeader);
    
    // remove everything after offset from currLeafData
    memset((char*)currLeafData + offset, 0, PAGE_SIZE - offset);    

    ixFileHandle.writePage(currPageNum, currLeafData);
    
    trafficPair.key = middleKey;
    trafficPair.pageNum = ixFileHandle.getNumberOfPages(); //this is pageNum of newLeafData (split page)

    free(newLeafData);
    free(middleKey);
    
    return SUCCESS;
}

RC IndexManager::splitInternalPage(void * currInternalData, unsigned currPageNum, IXFileHandle &ixFileHandle, Attribute attr, TrafficPair &trafficPair) {
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
    void *middleKey = malloc(keyLength);
    memcpy(middleKey, (char*)currInternalData + offset, keyLength);

    //calculate the chunk size of the split data
    unsigned splitDataSize = PAGE_SIZE - offset;

    // initialize new Internal Page
    void *newInternalData = malloc(PAGE_SIZE);
    newInternalPage(newInternalData);
    InternalPageHeader newInternalPageHeader = getInternalPageHeader(newInternalData);
    newInternalPageHeader.numEntries = currInternalPageHeader.numEntries - currNumEntries - 1; // -1 so you subtract the middle key entry
    newInternalPageHeader.FSO = sizeof(InternalPageHeader) + splitDataSize;
    newInternalPageHeader.leftChildPage = ixFileHandle.getNumberOfPages() + 1;
    setInternalPageHeader(newInternalData, newInternalPageHeader);

    memcpy((char*)newInternalData + sizeof(InternalPageHeader), (char*)currInternalData + offset, splitDataSize);
    
    ixFileHandle.appendPage(newInternalData);
    
    // Update the current leaf page to remove everything that was split from the page
    currInternalPageHeader.numEntries = currNumEntries; 
    currInternalPageHeader.FSO = offset; 
    setInternalPageHeader(currInternalData, currInternalPageHeader);
        
    // remove everything after offset and middle key from currLeafData
    memset((char*)currInternalData+offset, 0, PAGE_SIZE - offset); 

    ixFileHandle.writePage(currPageNum, currInternalData);

    trafficPair.key = middleKey;
    trafficPair.pageNum = newInternalPageHeader.leftChildPage; //this is pageNum of newInternalData (split page)

    // edge case: check if the page is a root
    unsigned rootPageNum = getRootPageNum(ixFileHandle);
    if (currPageNum == rootPageNum) {
        void *newRootData = malloc(PAGE_SIZE);
        newInternalPage(newRootData);
        InternalPageHeader newInternalPageHeader = getInternalPageHeader(newRootData);
        newInternalPageHeader.numEntries = 1; 
        newInternalPageHeader.FSO = keyLength + sizeof(PageNum) + sizeof(InternalPageHeader);
        newInternalPageHeader.leftChildPage = rootPageNum;
        setInternalPageHeader(newRootData, newInternalPageHeader);
        ixFileHandle.appendPage(newRootData);

        unsigned newRootNum = ixFileHandle.getNumberOfPages();
        
        // Insert the traffic pair to the new root page
        memcpy((char*)newRootData + sizeof(InternalPageHeader), trafficPair.key, keyLength);
        memcpy((char*)newRootData + sizeof(InternalPageHeader) + keyLength, &trafficPair.pageNum, sizeof(PageNum));

        // Write changes to page
        ixFileHandle.writePage(newRootNum, newRootData);

        // Get the meta data page
        void *metaDataPage = malloc(PAGE_SIZE);
        ixFileHandle.readPage(0, metaDataPage);
        MetaPageHeader metaDataHeader = getMetaPageHeader(metaDataPage);

        // update MetaData header
        metaDataHeader.rootNum = newRootNum;
        setMetaPageHeader(metaDataPage, metaDataHeader);

        ixFileHandle.writePage(0, metaDataPage);

        // free memory
        free(newRootData);
        free(metaDataPage);
    }

    free(newInternalData);
    
    return SUCCESS;
}

int IndexManager::compareKeys(Attribute attr, const void* key1, const void* key2) const {
    int valueInt1;
    int valueInt2;

    float valueFloat1;
    float valueFloat2;

    int stringLength1;
    int stringLength2;
    char* str1;
    char* str2;

    // cout << "get in compare keys\n";
    // unsigned keyLen = getKeyLength(key1, attr);
    // unsigned temp;
    // memcpy(&temp, (char*)key1 + sizeof(unsigned), 4+sizeof(RID));
    // cout << "key1: " << temp << endl;

    // keyLen = getKeyLength(key2, attr);
    // unsigned pls = 0;
    // memcpy(&pls, (char*)key2 + sizeof(unsigned), 4+sizeof(RID));
    // cout << "lowKey: " << pls << endl;
    

    switch (attr.type) {
        case TypeInt:
            memcpy(&valueInt1, key1, sizeof(int));
            
            memcpy(&valueInt2, key2, sizeof(int));

            if (valueInt1 < valueInt2) {
                return -1;
            } else if (valueInt1 > valueInt2) {
                return 1;
            } else {
                return 0;
            }

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
    return SUCCESS;
}

bool IndexManager::compareRIDS(const RID &rid1, const RID &rid2) const {
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

        // cout << tempRID.pageNum << endl;
        // cout << tempRID.slotNum << endl;
		
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
    InternalPageHeader rootPageHeader = getInternalPageHeader(rootPageData);
    rootPageHeader.leftChildPage = 2;
    setInternalPageHeader(rootPageData, rootPageHeader);

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
    // ixFileHandle already in use
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

    // printInternalPageHeader(internalPageHeader);
    //at this point, trafficPair shouldn't have null fields


    if (PAGE_SIZE - internalPageHeader.FSO < getKeyLength(key, attr) + sizeof(PageNum)) {
        return NO_SPACE;
    }

    //find the correct index
    unsigned offset = sizeof(InternalPageHeader);
    for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            break;
        }
        offset += getKeyLength((char*)pageData + offset, attr) + sizeof(PageNum);
    }

    //shift to the right to make space for new key, pageNum
    unsigned shiftOffset = offset + getKeyLength((char*)pageData + offset, attr) + sizeof(PageNum);
    memcpy((char*)pageData + shiftOffset, (char*)pageData + offset, internalPageHeader.FSO - offset);
    
    //insert the new trafficPair
    memcpy((char*)pageData + offset, key, getKeyLength(key, attr));
    memcpy((char*)pageData + offset + getKeyLength(key, attr), &pageNum, sizeof(PageNum));

    internalPageHeader.FSO += getKeyLength(key, attr) + sizeof(PageNum);
    internalPageHeader.numEntries += 1;

    setInternalPageHeader(pageData, internalPageHeader);

    return SUCCESS;
}

RC IndexManager::insertLeafPair(void *pageData, const Attribute &attr, const void *key, const RID &rid) {
    //we don't want duplicates
    if (leafPairExists(pageData, key, rid, attr)) {
        cout << "duplicate" << endl;
        return DUPLICATE;
    }

    LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
    
    // printLeafPageHeader(leafPageHeader);

    if (PAGE_SIZE - leafPageHeader.FSO < getKeyLength(key, attr) + sizeof(RID)) {
        return NO_SPACE;
    }
    
    unsigned offset = sizeof(LeafPageHeader);
    for (unsigned i = 0; i < leafPageHeader.numEntries; i++) {
        if (compareKeys(attr, key, (char*)pageData + offset) < 0) {
            break;
        }
        offset += getKeyLength((char*)pageData + offset, attr) + sizeof(RID);
    }
    // cout << "Offset: " << offset << endl;

    //for entires that come after, shift to the right
    unsigned shiftOffset = offset;
    memcpy((char*)pageData + shiftOffset, (char*)pageData + offset, leafPageHeader.FSO - offset);
    
    //insert the new trafficPair
    memcpy((char*)pageData + offset, key, getKeyLength(key, attr));
    memcpy((char*)pageData + offset + getKeyLength(key, attr), &rid, sizeof(RID));

    leafPageHeader.FSO += getKeyLength(key, attr) + sizeof(RID);
    leafPageHeader.numEntries += 1;

    setLeafPageHeader(pageData, leafPageHeader);

    return SUCCESS;
}

RC IndexManager::insert(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid, const unsigned pageNum, TrafficPair &trafficPair) {
    void *pageData = malloc(PAGE_SIZE);
    //start from the root page 
    if (ixfileHandle.readPage(pageNum, pageData) != SUCCESS) {
        cout << "couldn't read page" << endl;
        return 1;
    }
    // cout << "Flag: " << getFlag(pageData) << endl;
    //Internal Page
    
    RC rc;
    if (getFlag(pageData) == INTERNAL) {
        PageNum childPageNum = getChildPageNum(key, pageData, attr);
        // cout << "ChildPageNum: " << childPageNum << endl;
        rc = insert(ixfileHandle, attr, key, rid, childPageNum, trafficPair);
        if (trafficPair.key == NULL) {
            free(pageData);
            return SUCCESS;
        }

        //since trafficPair does not have NULL key, we have split a leaf page
        rc = insertInternalPair(pageData, attr, trafficPair.key, trafficPair.pageNum);
        if (rc == DUPLICATE) {
            return DUPLICATE;
        }

        if (rc == SUCCESS) {
            ixfileHandle.writePage(pageNum, pageData);
            free(pageData);
            trafficPair.key = NULL;
            return SUCCESS;
        }

        if (rc == NO_SPACE) {
            splitInternalPage(pageData, pageNum, ixfileHandle, attr, trafficPair);
            //try inserting again after splitting
            rc = insertInternalPair(pageData, attr, trafficPair.key, trafficPair.pageNum);
            if (rc == SUCCESS) {
                ixfileHandle.writePage(pageNum, pageData);
                free(pageData);
                trafficPair.key = NULL;
                return SUCCESS;
            }
            else 
                return SPLIT_ERROR;
        }
    }

    //Leaf Page
    else if (getFlag(pageData) == LEAF) {
        // cout << "i got to leaf" << endl;
        //try inserting and find out if there's free space
        rc = insertLeafPair(pageData, attr, key, rid);
        if (rc == DUPLICATE) {
            return DUPLICATE;
        }

        if (rc == SUCCESS) {
            ixfileHandle.writePage(pageNum, pageData);
            free(pageData);
            return SUCCESS;
        }

        if (rc == NO_SPACE) {
            splitLeafPage(pageData, pageNum, ixfileHandle, attr, trafficPair);
            //try inserting again after splitting
            rc = insertLeafPair(pageData, attr, key, rid);
            if (rc == SUCCESS) {
                ixfileHandle.writePage(pageNum, pageData);
                free(pageData);
                trafficPair.key = NULL;
                return SUCCESS;
            }
            else
                return SPLIT_ERROR;
        }  
    }
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{   
    //we might need to split, so we pass in a NULL traffic cop
    TrafficPair trafficPair;
    trafficPair.key = NULL;
    trafficPair.pageNum = UINT_MAX;

    PageNum rootPageNum = getRootPageNum(ixfileHandle);
    

    //recursive calls until alll splits and inserts are complete
    return insert(ixfileHandle, attribute, key, rid, rootPageNum, trafficPair);

}

RC IndexManager::search(PageNum &pageNum, PageNum &resultPageNum, IXFileHandle &ixfileHandle, Attribute attribute, const void *searchKey) {
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, pageData);
    
    if (getFlag(pageData) == LEAF) {
        resultPageNum = pageNum;
        return SUCCESS;
    }

    PageNum childPageNum = getChildPageNum(searchKey, pageData, attribute);
    
    free(pageData);
    return search(childPageNum, resultPageNum, ixfileHandle, attribute, searchKey);
}

// when i wrote this code only god and i knew how it worked. now, only god knows.
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    PageNum pageNum = getRootPageNum(ixfileHandle);
    PageNum leafPageNum = 2;

    search(pageNum, leafPageNum, ixfileHandle, attribute, key);
    
    cout << "After search: " << pageNum << endl;

    
    cout << "LeafPage: " << leafPageNum << endl;

    //we got to the leaf page
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(leafPageNum, pageData);
    
    //we can't delete something that doesn't exist
    if (!leafPairExists(pageData, key, rid, attribute)) {
        return -1;
    }

    LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
    unsigned offset = sizeof(LeafPageHeader);
    unsigned keyLength = 0;
    for (int i = 0; i < leafPageHeader.numEntries; i++) {
        if (compareKeys(attribute, key, (char*)pageData + offset) == 0) {
            break;
        }
        
        keyLength = getKeyLength((char*)pageData + offset, attribute);
        offset += keyLength + sizeof(RID);
    }

    cout << "keyLength: " << keyLength << endl;
    cout << "offset: " << offset << endl;

    //overwrite by shifting to the left
    unsigned chunkSize = leafPageHeader.FSO - (offset + keyLength + sizeof(RID));
    cout << "FSO: " << leafPageHeader.FSO << endl;
    cout << "ChunkSize: " << chunkSize << endl;

    memcpy((char*)pageData + offset, (char*)pageData + offset + keyLength + sizeof(RID), chunkSize);

    leafPageHeader.FSO -= keyLength - sizeof(RID);
    leafPageHeader.numEntries -= 1;

    setLeafPageHeader(pageData, leafPageHeader);

    ixfileHandle.writePage(leafPageNum, pageData);

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
    return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::printKey(const Attribute &attribute, void *pageData, unsigned offset) const{
    switch (attribute.type) {
        case TypeInt:
            int intKey;
            memcpy(&intKey, (char*)pageData + offset, sizeof(int));
            cout << intKey;
            break;
        case TypeReal:
            float floatKey;
            memcpy(&floatKey, (char*)pageData + offset, sizeof(float));
            cout << floatKey;
            break;
        case TypeVarChar:
            int stringLength;
            memcpy(&stringLength, (char*)pageData + offset, sizeof(int));
            char *stringKey = (char*)malloc(stringLength + 1);
            memcpy(stringKey, (char*)pageData + offset + sizeof(int), stringLength);
            stringKey[stringLength] = '\0';
            cout << stringKey;
            break;
    }

    
}

void IndexManager::printRID(const Attribute &attribute, void *pageData, unsigned &offset) const{
    int startKeyLength = getKeyLength((char*)pageData + offset, attribute);
    void *startKey = malloc(startKeyLength);
    memcpy(startKey, (char*)pageData + offset, startKeyLength);

    cout << "[";
    RID rid;
    while (true) {
        memcpy(&rid, (char*)pageData + offset + startKeyLength, sizeof(RID));
        cout << "(" << rid.pageNum << "," << rid.slotNum << ")";

        //update offset to start of next key
        offset += startKeyLength + sizeof(RID);
        
        if (compareKeys(attribute, startKey, (char*)pageData + offset) != 0) {
            cout << "]\"";
            break;
        }
        

        cout << ",";
    }

    cout << offset;
}


void IndexManager::preorder(IXFileHandle &ixFileHandle, PageNum pageNum, const Attribute &attribute, int depth) const {
    void *pageData = malloc(PAGE_SIZE);
    ixFileHandle.readPage(pageNum, pageData);

    //Base Case
    if (getFlag(pageData) == LEAF) {
        LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
        unsigned offset = sizeof(LeafPageHeader);
        cout << string(depth, '\t') << "{\"keys\":[";

        for (int i = 0; i < leafPageHeader.numEntries; i++) {
            cout << "\"";
            printKey(attribute, pageData, offset);
            cout << ":";
            
            printRID(attribute, pageData, offset);
            if (i + 1 >= leafPageHeader.numEntries) {
                cout << "]}," << endl;
            }
        }
        
    }

    else if (getFlag(pageData) == INTERNAL) {
        InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);
        unsigned offset = sizeof(InternalPageHeader);
        for (int i = 0; i < internalPageHeader.numEntries; i++) {
            PageNum childPageNum ;
            unsigned currKeyLength = getKeyLength((char*)pageData + offset, attribute);
            memcpy(&childPageNum, (char*)pageData + offset + currKeyLength, sizeof(PageNum));
            preorder(ixFileHandle, childPageNum, attribute, depth + 1);
        }
    }

    free(pageData);
    
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    
    PageNum rootPageNum = getRootPageNum(ixfileHandle);
    
    cout << "{" << endl;
    preorder(ixfileHandle, rootPageNum, attribute, 0);
    cout << "}" << endl;
    
}

IX_ScanIterator::IX_ScanIterator()
: currPage(0), currKey(0), totalPage(0), totalNumEntries(0), currOffset(0)
{
    _indexManager = IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator()
{

}

RC IX_ScanIterator::getFirstLeafPage(PageNum &pageNum, PageNum &resultPageNum) {
    ixfileHandle.readPage(pageNum, _pageData);
    if (_indexManager->getFlag(_pageData) == LEAF) {
        resultPageNum = pageNum;
        return SUCCESS;
    }

    if (_indexManager->getFlag(_pageData) == INTERNAL) {
        InternalPageHeader internalPageHeader = _indexManager->getInternalPageHeader(_pageData);
        PageNum childPageNum = internalPageHeader.leftChildPage;
        return getFirstLeafPage(childPageNum, resultPageNum);
    }
    return SUCCESS;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    _indexManager = IndexManager::instance();

    void * temp = currKey;
    RID ridTemp = currRid;

    // check if we need to read in a new page
    if (currEntry == totalNumEntries) { // then its time to read in a new page 
        if (currPage == totalPage || leafPageHeader.next == UINT_MAX || leafPageHeader.next == 0) {
            free(currKey);
            return IX_EOF;
        }
        currPage = leafPageHeader.next;
        ixfileHandle.readPage(currPage, _pageData); // read in next Page
        leafPageHeader = _indexManager->getLeafPageHeader(_pageData); // store newest leafPageHeader
        totalNumEntries = leafPageHeader.numEntries;
        currEntry = 0;
        currOffset = sizeof(LeafPageHeader);
    }
    
    // Get currKey
    unsigned keyLength = _indexManager->getKeyLength((char*)_pageData+currOffset, attr);
    currKey = malloc(keyLength); // is key already malloc'd?
    memcpy(currKey, (char*)_pageData + currOffset, keyLength);

    // Get currRid
    memcpy(&currRid, (char*)_pageData + currOffset + keyLength, sizeof(RID));

    // For each key, call compareKey on currKey, highKey if (highKey != NULL)
    if (highKey != NULL) {
        if (_indexManager->compareKeys(attr, currKey, highKey) == 0 && !highKeyInclusive) {
            key = temp;
            rid = ridTemp;
            free(currKey);
            return IX_EOF;
        }
        else if (_indexManager->compareKeys(attr, currKey, highKey) == 0 ) {
            key = currKey;
            rid = currRid;
            free(currKey);
            return IX_EOF;
        }
    }
    
    key = currKey;
    rid = currRid;

    currOffset += _indexManager->getKeyLength((char*)_pageData+currOffset, attr) + sizeof(RID); 
    currEntry++;    


    return SUCCESS;
}

RC IX_ScanIterator::close()
{
    free(_pageData);
    return SUCCESS;
}

RC IndexManager::ixFileHandleExists(IXFileHandle &ixfileHandle) {
    // ixFileHandle already in use
    if (ixfileHandle.getfd() != NULL) {
        return 1;
    }
    return SUCCESS;
}

RC IX_ScanIterator::scanInit(IXFileHandle &ixFh, const Attribute &attribute, const void*lK, const void *hK, bool lKI, bool hKI) {
    // check if IXFileHandle exists
    if (!_indexManager->ixFileHandleExists(ixFh)) {
        return 1;
    }

    // Start at root page
    currOffset = sizeof(LeafPageHeader);
    currPage = 0;
    
    currKey = NULL;
    currRid.pageNum = 0;
    currRid.slotNum = 0;
    currEntry = 0;
    
    // Buffer to hold the current page
    _pageData = malloc(PAGE_SIZE);

    // Store the variables passed in
    ixfileHandle = ixFh;
    attr = attribute;
    lowKey = lK;
    highKey = hK;
    lowKeyInclusive = lKI;
    highKeyInclusive = hKI;

    totalPage = ixFh.getNumberOfPages();
    // read in the page
    if (totalPage > 0) {
        if (ixFh.readPage(currPage, _pageData)) {
            RBFM_READ_FAILED;
        }
    } else 
        return SUCCESS;


    PageNum rootNum = _indexManager->getRootPageNum(ixfileHandle);


    if (lowKey == NULL) {
        PageNum rootNum = _indexManager->getRootPageNum(ixfileHandle);
        getFirstLeafPage(rootNum, currPage);
    } else 
        _indexManager->search(rootNum, currPage, ixfileHandle, attr, lowKey);

    leafPageHeader = _indexManager->getLeafPageHeader(_pageData);
    totalNumEntries = leafPageHeader.numEntries;

    
    // if lowKey != NULL, get offset to be right where lowKey starts
    if (lowKey != NULL) {
        while (true) {
            unsigned keyLength = _indexManager->getKeyLength((char*)_pageData+currOffset, attribute);
            // currKey = malloc(keyLength); // is key already malloc'd?
            // memcpy(currKey, (char*)_pageData + currOffset, keyLength);
            // cout << "Does compareKeys return?" << endl;
            if(_indexManager->compareKeys(attr, (char*)_pageData+currOffset, lowKey) == 0 && lowKeyInclusive) {
                break;
            }
            
            if (_indexManager->compareKeys(attr, (char*)_pageData+currOffset, lowKey) > 0 && !lowKeyInclusive) {
                break;
            }
            currOffset += keyLength + sizeof(RID);
        }
    }
    
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