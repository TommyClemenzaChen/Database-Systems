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
    
    cout << "[Test] Num pages at init: " << ixfileHandle.getNumberOfPages() << endl;

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

    cout << "[Test] Leaf num entries before loop: " << leafPageHeader.numEntries << endl;

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

    // // Optionally write the modified page back to the file
    // ixfileHandle.writePage(0, data);

    cout << "[Test] Num entries after loop: " << leafPageHeader.numEntries << endl;

    cout << "Num pages before: " << ixfileHandle.getNumberOfPages() << endl;

    indexManager->splitLeafPage(data, 1, ixfileHandle, attribute, trafficPair);

    cout << "Num pages after: " << ixfileHandle.getNumberOfPages() << endl;

    // int a;
    // memcpy(&a, trafficPair.key, sizeof(int));
    // cout << endl << "Traffic pair key: " << a << endl << "Traffic pair pageNum: " << trafficPair.pageNum << endl;

    free(data);
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


int main () {
    indexManager = IndexManager::instance();
    
    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeVarChar;

    // testCompareKeys(indexManager);

    cout << "--------------------------------------------------------------------------" << endl;

    //populate the files
    
    RC result = testSplitLeafPage(indexFileName, attrAge);
    
    cout << "--------------------------------------------------------------------------" << endl;
    // cout << result << endl;
    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}