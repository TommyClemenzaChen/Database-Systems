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
    unsigned childPageNum = 2;
    InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);

    //keep reading until we find the correct child
    unsigned offset = sizeof(InternalPageHeader);
    
    for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
        unsigned keyLength = getKeyLength(key, attr);
        memcpy(&childPageNum, (char*)pageData + offset + keyLength, sizeof(PageNum));
        
        if (compareKeys(attr, key, (char*)pageData + offset) == -1) {
            return childPageNum;
        } //we need to find where key is smaller

        offset += keyLength + sizeof(PageNum);
    }

    cout << "childPage: " << childPageNum;
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

RC IndexManager::splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr, TrafficPair &trafficPair) {
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
    
    void *middleKey = malloc(keyLength /*+ sizeof(PageNum)*/);
    
    memcpy(middleKey, (char*)currLeafData + offset, keyLength);

    // Split all the data that comes after offset and middle key into newInternalData
    unsigned splitOffset = offset + keyLength + sizeof(PageNum);

    // initialize new leaf page
    void *newLeafData = malloc(PAGE_SIZE);
    newLeafPage(newLeafData);
    LeafPageHeader newLeafPageHeader = getLeafPageHeader(newLeafData);
    newLeafPageHeader.FSO = (PAGE_SIZE- splitOffset) - sizeof(LeafPageHeader);
    newLeafPageHeader.numEntries = currLeafPageHeader.numEntries - currNumEntries -1;
    newLeafPageHeader.next = UINT_MAX; // some number that we can't reach
    newLeafPageHeader.prev = currPageNum;
    setLeafPageHeader(newLeafData, newLeafPageHeader);

    // Split all the data that comes after offset into newLeafData
    memcpy((char*)newLeafData + sizeof(LeafPageHeader), (char*)currLeafData + splitOffset, PAGE_SIZE - splitOffset);
    
    ixFileHandle.appendPage(newLeafData);
    
    // Update the current leaf page to remove everything that was split from the page
    currLeafPageHeader.numEntries = currNumEntries; 
    currLeafPageHeader.FSO = offset; 
    currLeafPageHeader.next = ixFileHandle.getNumberOfPages();
    setLeafPageHeader(currLeafData, currLeafPageHeader);
    
    // remove everything after offset from currLeafData
    memset((char*)currLeafData + offset, 0, PAGE_SIZE - offset);    

    ixFileHandle.writePage(currPageNum, currLeafData);
    
    trafficPair.key = middleKey;
    trafficPair.pageNum = ixFileHandle.getNumberOfPages(); //this is pageNum of newLeafData (split page)

    // free(newLeafData);
    // free(middleKey);
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
    void *middleKey = malloc(keyLength /*+ sizeof(PageNum)*/);
    memcpy(middleKey, (char*)currInternalData + offset, keyLength);

    // Split all the data that comes after offset and middle key into newInternalData
    unsigned splitOffset = offset + keyLength + sizeof(PageNum);
    
    // initialize new Internal Page
    void *newInternalData = malloc(PAGE_SIZE);
    newInternalPage(newInternalData);
    InternalPageHeader newInternalPageHeader = getInternalPageHeader(newInternalData);
    newInternalPageHeader.numEntries = currInternalPageHeader.numEntries - currNumEntries - 1; // -1 so you subtract the middle key entry
    newInternalPageHeader.FSO = sizeof(InternalPageHeader) + (PAGE_SIZE - splitOffset);
    setInternalPageHeader(newInternalData, newInternalPageHeader);

    memcpy((char*)newInternalData + sizeof(InternalPageHeader), (char*)currInternalData + splitOffset, PAGE_SIZE - splitOffset);
    
    ixFileHandle.appendPage(newInternalData);
    
    // Update the current leaf page to remove everything that was split from the page
    currInternalPageHeader.numEntries = currNumEntries; 
    currInternalPageHeader.FSO = offset; 
    setInternalPageHeader(currInternalData, currInternalPageHeader);
        
    // remove everything after offset and middle key from currLeafData
    memset((char*)currInternalData+offset, 0, PAGE_SIZE - offset); 

    ixFileHandle.writePage(currPageNum, currInternalData);

    trafficPair.key = middleKey;
    trafficPair.pageNum = ixFileHandle.getNumberOfPages(); //this is pageNum of newInternalData (split page)

    // edge case: check if the page is a root
    unsigned rootPageNum = getRootPageNum(ixFileHandle);
    if (currPageNum == rootPageNum) {
        void *newRootData = malloc(PAGE_SIZE);
        newInternalPage(newRootData);
        InternalPageHeader newInternalPageHeader = getInternalPageHeader(newRootData);
        newInternalPageHeader.numEntries = 1; 
        newInternalPageHeader.FSO = keyLength + sizeof(PageNum) + sizeof(InternalPageHeader);
        setInternalPageHeader(newRootData, newInternalPageHeader);
        ixFileHandle.appendPage(newRootData);

        unsigned newRootNum = ixFileHandle.getNumberOfPages(); // this might be getNumPages+1 (this could be causing errors elsewhere)

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
        // free(newRootData);
        // free(metaDataPage);
    }

    // free(newInternalData);
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

    printInternalPageHeader(internalPageHeader);
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

    //for entires that come after, shift to the right
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
    
    printLeafPageHeader(leafPageHeader);

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
    cout << "Offset: " << offset << endl;

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
    cout << "Flag: " << getFlag(pageData) << endl;
    //Internal Page
    
    RC rc;
    if (getFlag(pageData) == INTERNAL) {
        PageNum childPageNum = getChildPageNum(key, pageData, attr);
        cout << "ChildPageNum: " << childPageNum << endl;
        rc = insert(ixfileHandle, attr, key, rid, childPageNum, trafficPair);
        if (trafficPair.key == NULL) {
            //check to see if we are trying to insert at first leaf and there's no traffic cop
            if (getInternalPageHeader(pageData).numEntries == 0 && pageNum == 1) {
                insertInternalPair(pageData, attr, key, childPageNum);
                ixfileHandle.writePage(pageNum, pageData);
            }
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
        cout << "i got to leaf" << endl;
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

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *pageData = malloc(PAGE_SIZE);
    PageNum rootPageNum = getRootPageNum(ixfileHandle);
    
    ixfileHandle.readPage(rootPageNum, pageData);
    InternalPageHeader internalPageHeader;
    
    //traverse through the file tree
    PageNum currPageNum = rootPageNum; //starting at root
    unsigned offset;
    unsigned keyLength;

    while (true) {
        //we are at a leaf page
        if (getFlag(pageData) == LEAF) {
            break;
        }
        offset = sizeof(InternalPageHeader);
        internalPageHeader = getInternalPageHeader(pageData);
        
        for (int i = 0; i < internalPageHeader.numEntries; i++) {
            keyLength = getKeyLength((char*)pageData, attribute);
            memcpy(&currPageNum, (char*)pageData + offset + keyLength, sizeof(PageNum));
            
            if (compareKeys(attribute, key, (char*)pageData + offset) < 0) {
                break;
            }
            offset += keyLength + sizeof(PageNum);
        }
        
        //read in next page
        ixfileHandle.readPage(currPageNum, pageData);
    }

    PageNum leafPageNum = currPageNum;
    cout << "LeafPage: " << leafPageNum << endl;

    //we got to the leaf page
    ixfileHandle.readPage(leafPageNum, pageData);
    
    //we can't delete something that doesn't exist
    if (!leafPairExists(pageData, key, rid, attribute)) {
        return SUCCESS;
    }

    LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
    offset = sizeof(LeafPageHeader);
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

void IndexManager::printKey(const Attribute &attribute, void *pageData, unsigned offset, unsigned &keyLength) const{
    switch (attribute.type) {
        case TypeInt:
            int intKey;
            memcpy(&intKey, (char*)pageData + offset, sizeof(int));
            cout << intKey;
            keyLength = sizeof(int);
            break;
        case TypeReal:
            float floatKey;
            memcpy(&floatKey, (char*)pageData + offset, sizeof(float));
            cout << floatKey;
            keyLength = sizeof(float);
            break;
        case TypeVarChar:
            int stringLength;
            memcpy(&stringLength, (char*)pageData + offset, sizeof(int));
            char *stringKey = (char*)malloc(stringLength + 1);
            memcpy(stringKey, (char*)pageData + offset, stringLength);
            stringKey[stringLength] = '\0';
            cout << stringKey;
            keyLength = sizeof(int) + stringLength;
            break;
    }

    
}

void IndexManager::printRID(void *pageData, unsigned offset) const{
    RID rid;
    memcpy(&rid, (char*)pageData + offset, sizeof(RID));
    cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
}


void IndexManager::preorder(IXFileHandle &ixFileHandle, PageNum pageNum, const Attribute &attribute, unsigned &keyLength) const {
    void *pageData = malloc(PAGE_SIZE);
    ixFileHandle.readPage(pageNum, pageData);

    //Base Case
    if (getFlag(pageData) == LEAF) {
        LeafPageHeader leafPageHeader = getLeafPageHeader(pageData);
        unsigned offset = sizeof(LeafPageHeader);
        
        cout << "\t{\"keys\":[";
        do {
            cout << "\"";
            printKey(attribute, pageData, offset, keyLength);
            cout << ":[";
            for (unsigned i = 0; i < leafPageHeader.numEntries; i++) {
                printRID(pageData, offset);
                if (i + 1 < leafPageHeader.numEntries) {
                    cout << ",";
                }
            }
            cout << "]\"";
            if (leafPageHeader.next != UINT_MAX)
                cout << ",";
        } while (leafPageHeader.next != UINT_MAX);
        
        cout << "]}";
    }

    else if (getFlag(pageData) == INTERNAL) {
        InternalPageHeader internalPageHeader = getInternalPageHeader(pageData);
        unsigned offset = sizeof(InternalPageHeader);
    
        cout << "\"keys\":[";
        for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
            if (i + 1 < internalPageHeader.numEntries) {
                cout << ",";
            }
            cout << "\"";
            printKey(attribute, pageData, offset, keyLength);
            cout << "\"";
        }
        cout << "]," << endl;

        cout << "\"children\":[" << endl;
        
        for (unsigned i = 0; i < internalPageHeader.numEntries; i++) {
            PageNum childPageNum = getChildPageNum((char*)pageData + offset, pageData, attribute);
            preorder(ixFileHandle, childPageNum, attribute, keyLength);
            if (i + 1< internalPageHeader.numEntries) {
                cout << ",";
            }
        }
        cout << endl << "]" << endl;
        
    }

    free(pageData);
    
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {

/*
    void *pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(1, pageData);
    PageNum pageNum;
    memcpy(&pageNum, (char*)pageData + sizeof(InternalPageHeader) + sizeof(int), sizeof(PageNum));
    cout << pageNum << endl;

    ixfileHandle.readPage(2, pageData);
    int key;
    memcpy(&key, (char*)pageData + sizeof(LeafPageHeader), sizeof(int));
    cout << key << endl;
    RID rid;
    memcpy(&rid, (char*)pageData + sizeof(LeafPageHeader) + sizeof(int), sizeof(RID));
    cout << rid.pageNum << " | " << rid.slotNum << endl;
*/
    // void *key = NULL;
    unsigned keyLength;
    
    cout << "{" << endl;
    preorder(ixfileHandle, 1, attribute, keyLength);
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

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    _indexManager = IndexManager::instance();

    // you've read the current page in
    // now you need to get the next entry on the page
    
    // get lowest leaf page first using lowKey, then read leaf pages from there since they're linked

    // read through leaf page
    // when global num entries 

    // have a function that finds the first lowKey entry and returns the pageNum (do we guarantee it exists? if not, use if LeafPairExists())
    
    // have a global LeafPageHeader var that corresponds to _pageData
    cout << "Current entry: " << currEntry << ", total Entries: " << totalNumEntries << endl;
    if (currEntry == totalNumEntries) { // then its time to read in a new page 
        if (leafPageHeader.next == UINT_MAX || leafPageHeader.next == 0) {
            return IX_EOF;
        }
        currPage = leafPageHeader.next;
        ixfileHandle.readPage(currPage, _pageData); // read in next Page
        leafPageHeader = _indexManager->getLeafPageHeader(_pageData); // store newest leafPageHeader
        totalNumEntries = leafPageHeader.numEntries;
        currEntry = 0;
        currOffset = sizeof(LeafPageHeader);
    }

    currOffset += _indexManager->getKeyLength((char*)_pageData+currOffset, attr) + sizeof(RID); 
    currEntry++;    
    
    // Get currKey
    unsigned keyLength = _indexManager->getKeyLength((char*)_pageData+currOffset, attr);
    currKey = malloc(keyLength); // is key already malloc'd?
    memcpy(currKey, (char*)_pageData + currOffset, keyLength);

    // Get currRid
    memcpy(&currRid, (char*)_pageData + currOffset + keyLength, sizeof(RID));

    // For each key, call compareKey on currKey, highKey if (highKey != NULL)
    if (highKey != NULL) {
        if (_indexManager->compareKeys(attr, currKey, highKey) == 0 && !highKeyInclusive) {
            return IX_EOF;
        }
        else if (_indexManager->compareKeys(attr, currKey, highKey) == 0) {
            key = currKey;
            rid = currRid;
        }
    }

    return SUCCESS;
}

unsigned IX_ScanIterator::getLowKeyPage() {
    if (lowKey == NULL) return SUCCESS;
    // find first lowkey page and return pageNum

    return SUCCESS; 
}

unsigned IX_ScanIterator::getFirstLeafPage() {
    return searchLeaf(NULL);
    
    // return SUCCESS;
}

// malloc for page data outside of the function
unsigned IX_ScanIterator::searchLeaf(void* key) {
    // returns the page that the key is on 
    void* internalPageData = malloc(PAGE_SIZE);

    // always start at the root
    unsigned rootNum = _indexManager->getRootPageNum(ixfileHandle);
    cout << "Num read accesses? : " << ixfileHandle.ixReadPageCounter << endl;
    ixfileHandle.readPage(rootNum, internalPageData); // read rootPage
    InternalPageHeader internalPageHeader = _indexManager->getInternalPageHeader(internalPageData);

    if (lowKey == NULL) {
        free(internalPageData);
        return 2;
    }

    unsigned offset = sizeof(InternalPageHeader);
    // void *currentKey; // do i malloc here ? fml
    unsigned keyLength;
    unsigned nextPage = 0;
    while (_indexManager->getFlag(internalPageData ) == LEAF) {
        keyLength = _indexManager->getKeyLength((char*)internalPageData+offset, attr);
        cout << "key length: " << keyLength << endl;
        memcpy(&nextPage, (char*)internalPageData+offset+keyLength, sizeof(PageNum));
        cout << "Next page: " << nextPage << endl;
        // try to find what page the leaf key is at and read it in
        ixfileHandle.readPage(nextPage, internalPageData);
    }
    free(internalPageData);
    cout << "Next page: " << nextPage << endl;
    return nextPage;
}

RC IX_ScanIterator::close()
{
    free(_pageData);
    return SUCCESS;
}

RC IX_ScanIterator::scanInit(IXFileHandle &ixFh, const Attribute &attribute, const void*lK, const void *hK, bool lKI, bool hKI) {
    // Start at root page
    currOffset = sizeof(InternalPageHeader);
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

    // if (lowKey != NULL) {
    //     cout << "in here" << endl;
    //     currPage = getLowKeyPage(); 
    // }
    // else {
    //     cout << "in here2" << endl;
    currPage = getFirstLeafPage();

    cout << "First page: " << currPage << endl;
    totalPage = ixFh.getNumberOfPages();

    // read in the page
    if (totalPage > 0) {
        if (ixfileHandle.readPage(currPage, _pageData)) {
            RBFM_READ_FAILED;
        }
    } else return SUCCESS;

    leafPageHeader = _indexManager->getLeafPageHeader(_pageData);
    totalNumEntries = leafPageHeader.numEntries;

    cout << leafPageHeader.numEntries << endl;

    cout << "Total Num entries: " << totalNumEntries << endl;

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