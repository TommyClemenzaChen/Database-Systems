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
    
    RID rid;
    rid.pageNum = 1;
    rid.slotNum = 1;

    int stringLength = 5;
    string word = "Hello";
    

    void *data = malloc(PAGE_SIZE);
    LeafPageHeader leafPageHeader;
    indexManager->setLeafPageHeader(data, leafPageHeader);
    unsigned offset = sizeof(LeafPageHeader);
    
    while (true) {
        if (offset >= PAGE_SIZE) {
            break;
        }
        memcpy((char*)data + offset, &stringLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)data + offset, &word, stringLength);
        offset += stringLength;

    }
    
    cout << offset << endl;
    Attribute attr;
    attr.name = "age";
    attr.type = TypeInt;
    attr.length = 4;

    

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
    attrAge.type = TypeInt;

    testCompareKeys(indexManager);

    //populate the files
    

    RC result = testSplitLeafPage(indexFileName, attrAge);
    cout << result << endl;
    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}