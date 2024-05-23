#include <iostream>
#include <cstdio>
#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int main () {
    cout << "-------------------------------------------------------------------------" << endl;
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

    InternalPageHeader internalPageHeader = indexManager->getInternalPageHeader(pageData);
    cout << "Num entries before: " << internalPageHeader.numEntries << endl;
    cout << "FSO before: " << internalPageHeader.FSO << endl;

    // insert entry
    int num = 50; 
    void* key = malloc(sizeof(int));
    memcpy(key, &num, sizeof(int));

    RID rid;
    rid.pageNum = 1;
    rid.slotNum = 20;
    RC result = indexManager->insertEntry(ixfileHandle, attr, key, rid);
    
    rid.pageNum = 2;
    rid.slotNum = 30;
    result = indexManager->insertEntry(ixfileHandle, attr, key, rid);
    
    rid.pageNum = 3;
    rid.slotNum = 40;
    result = indexManager->insertEntry(ixfileHandle, attr, key, rid);

    num = 40;
    memcpy(key, &num, sizeof(int));
    rid.pageNum = 4;
    rid.slotNum = 50;
    result = indexManager->insertEntry(ixfileHandle, attr, key, rid);


    ixfileHandle.readPage(rootNum, pageData);
    internalPageHeader = indexManager->getInternalPageHeader(pageData);

    cout << "Num entries after: " << internalPageHeader.numEntries << endl;
    cout << "FSO after: " << internalPageHeader.FSO << endl;


    indexManager->printBtree(ixfileHandle, attr);

    cout << "-------------------------------------------------------------------------" << endl;
    
    // delete entry
    result = indexManager->deleteEntry(ixfileHandle, attr, &key, rid);

    ixfileHandle.readPage(rootNum, pageData);
    internalPageHeader = indexManager->getInternalPageHeader(pageData);

    cout << "Num entries after: " << internalPageHeader.numEntries << endl;
    cout << "FSO after: " << internalPageHeader.FSO << endl;

    cout << "-------------------------------------------------------------------------" << endl;

    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}