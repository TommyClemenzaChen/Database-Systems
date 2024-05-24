#include <iostream>
#include <cstdio>
#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int testSplitLeafPage(const string &indexFileName, const Attribute &attribute) {
    // open index file
    IXFileHandle ixfileHandle;
      
    indexManager->createFile(indexFileName);
    indexManager->openFile(indexFileName, ixfileHandle);
    
    // initialize empty traffic pair
    TrafficPair trafficPair;
    trafficPair.key = NULL;
    trafficPair.pageNum = 0;

    RID rid;
    rid.pageNum = 1;
    rid.slotNum = 1;

    int stringLength = 5;
    const char* word = "Hello";

    void *data = malloc(PAGE_SIZE);
    
    indexManager->newLeafPage(data);
    ixfileHandle.appendPage(data);

    LeafPageHeader leafPageHeader = indexManager->getLeafPageHeader(data);

    unsigned offset = sizeof(LeafPageHeader);
    
    while (true) {
        // Check if there is enough space to insert the key and RID
        if (offset + sizeof(int) + stringLength + sizeof(RID) > PAGE_SIZE) {
            break;
        }
        // Copy the string length
        memcpy((char *)data + offset, &stringLength, sizeof(int));
        offset += sizeof(int);

        // Copy the string data
        memcpy((char *)data + offset, word, stringLength);
        offset += stringLength;

        // Copy the RID
        memcpy((char *)data + offset, &rid, sizeof(RID));
        offset += sizeof(RID);

        // Update the number of entries and free space offset in the header
        leafPageHeader.numEntries += 1;
        leafPageHeader.FSO = offset;
    }
    
    // Update the leaf page header
    memcpy(data, &leafPageHeader, sizeof(LeafPageHeader));

    indexManager->splitLeafPage(data, 3, ixfileHandle, attribute, trafficPair);

    char *temp = (char*)malloc(10);
    memcpy(temp, (char*)trafficPair.key + sizeof(int), 9);
    temp[9] = '\0';
    cout << "Temp: " << temp << endl;

    free(data);
    return SUCCESS;
}


int testSplitInternalPage(const string &indexFileName, const Attribute &attribute) {
    // open index file
    IXFileHandle ixfileHandle;
      
    indexManager->createFile(indexFileName);
    indexManager->openFile(indexFileName, ixfileHandle);
    
    // initialize empty traffic pair
    TrafficPair trafficPair;
    trafficPair.key = NULL;
    trafficPair.pageNum = 0;
    
    // initialize varchar
    int stringLength = 5;
    const char* word = "Hello";

    // initialize PageNum
    PageNum pageNum = 2;

    void *data = malloc(PAGE_SIZE);
    ixfileHandle.readPage(1, data);
    
    // how do i get the root page number and then point to that?
    
    InternalPageHeader internalPageHeader = indexManager->getInternalPageHeader(data);

    unsigned offset = sizeof(internalPageHeader);
    
    while (true) {
        // Check if there is enough space to insert the key and RID
        if (offset + sizeof(int) + stringLength + sizeof(pageNum) > PAGE_SIZE) {
            break;
        }
        // Copy the string length
        memcpy((char *)data + offset, &stringLength, sizeof(int));
        offset += sizeof(int);

        // Copy the string data
        memcpy((char *)data + offset, word, stringLength);
        offset += stringLength;

        // Copy the PageNum
        memcpy((char *)data + offset, &pageNum, sizeof(PageNum));
        offset += sizeof(PageNum);

        // Update the number of entries and free space offset in the header
        internalPageHeader.numEntries += 1;
        internalPageHeader.FSO += offset;
    }

    // Update the leaf page header
    memcpy(data, &internalPageHeader, sizeof(internalPageHeader));

    
    indexManager->splitInternalPage(data, 1, ixfileHandle, attribute, trafficPair);

    unsigned rootPageNum = indexManager->getRootPageNum(ixfileHandle);

    unsigned keyLength = indexManager->getKeyStringLength(trafficPair.key);
    char *temp = (char*)malloc(keyLength + 1);
    memcpy(temp, (char*)trafficPair.key+sizeof(int), keyLength+sizeof(PageNum));
    temp[keyLength] = '\0';
    
    cout << endl << "TEST: Traffic pair key: " << temp << endl << "TEST: Traffic pair pageNum: " << trafficPair.pageNum << endl;


    free(temp);
    return SUCCESS;
}


void testCompareKeys(IndexManager *indexManager) {
     //test compareKeys
    Attribute attr = {"empName", TypeVarChar, 50};
    int len1 = 5;
    int len2 = 4;
    void* key1 = malloc(sizeof(int) + len1);
    void* key2 = malloc(sizeof(int) + len2);

    // Copy the length and the string data into key1
    memcpy((char*)key1, &len1, sizeof(int));
    memcpy((char*)key1 + sizeof(int), "hell", len1);

    // Copy the length and the string data into key2
    memcpy((char*)key2, &len2, sizeof(int));
    memcpy((char*)key2 + sizeof(int), "hell", len2);
    
    int rc = indexManager->compareKeys(attr, key1, key2);
    
    cout << "Compare Keys #1: " << rc << endl;

}

RC testPrintBtree(const string &indexFileName, const Attribute &attribute) {
    // open index file
    IXFileHandle ixfileHandle;
      
    indexManager->createFile(indexFileName);
    indexManager->openFile(indexFileName, ixfileHandle);
    
    indexManager->printBtree(ixfileHandle, attribute);


    return SUCCESS;
    
}

RC testSearch(const string &indexFileName, const Attribute &attribute) {

}

int main () {
    indexManager = IndexManager::instance();
    
    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeVarChar;

    // testCompareKeys(indexManager);

    // populate the files
    
    RC result = testSplitLeafPage(indexFileName, attrAge);

    result = testSplitInternalPage(indexFileName, attrAge);

    result = testPrintBtree(indexFileName, attrAge);
    
    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}