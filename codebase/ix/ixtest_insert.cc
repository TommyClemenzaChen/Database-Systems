#include <iostream>
#include <cstdio>
#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int main () {
    indexManager = IndexManager::instance();
    
    const string indexFileName = "age_idx";
    Attribute attr;
    attr.length = 4;
    attr.name = "age";
    attr.type = TypeInt;

    // open the file
    IXFileHandle ixfileHandle;
      
    indexManager->createFile(indexFileName);
    indexManager->openFile(indexFileName, ixfileHandle);

    // alloc memory for the page
    void *pageData = malloc(PAGE_SIZE);
    unsigned rootNum = indexManager->getRootPageNum(ixfileHandle);

    cout << "Root num: " << rootNum << endl;

    // read in the page
    ixfileHandle.readPage(rootNum, pageData);

    // create the key

    // initialize varchar
    // int stringLength = 5;
    // const char* word = "Hello";
    int num = 50; 
    void* key = malloc(sizeof(int));
    memcpy(key, &num, sizeof(int));

    RID rid;
    rid.pageNum = 1;
    rid.slotNum = 20;

    InternalPageHeader internalPageHeader = indexManager->getInternalPageHeader(pageData);
    cout << "Num entries before: " << internalPageHeader.numEntries << endl;


    // insert entry
    RC result = indexManager->insertEntry(ixfileHandle, attr, &key, rid);
    
    cout << "Num entries after: " << internalPageHeader.numEntries << endl;


    indexManager->printBtree(ixfileHandle, attr);


    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}