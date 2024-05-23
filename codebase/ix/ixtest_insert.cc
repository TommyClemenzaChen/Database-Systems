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
    int num = 50; 
    void* key = malloc(sizeof(int));
    memcpy(key, &num, sizeof(int));

    RID rid;
    rid.pageNum = 1;
    rid.slotNum = 20;

    InternalPageHeader internalPageHeader = indexManager->getInternalPageHeader(pageData);
    cout << "Num Internal entries before: " << internalPageHeader.numEntries << endl;
    cout << "Internal FSO before: " << internalPageHeader.FSO << endl;

    ixfileHandle.readPage(2, pageData);
    LeafPageHeader leafPageHeader = indexManager->getLeafPageHeader(pageData);
    cout << "Num leaf entries before: " << leafPageHeader.numEntries << endl;

    // insert entry
    RC result = indexManager->insertEntry(ixfileHandle, attr, &key, rid);

    ixfileHandle.readPage(2, pageData);
    leafPageHeader = indexManager->getLeafPageHeader(pageData);
    cout << "Num leaf entries after: " << leafPageHeader.numEntries << endl;


    // indexManager->printBtree(ixfileHandle, attr);

    cout << "-------------------------------------------------------------------------" << endl;
    
    // delete entry
    result = indexManager->deleteEntry(ixfileHandle, attr, &key, rid);

    ixfileHandle.readPage(2, pageData);
    leafPageHeader = indexManager->getLeafPageHeader(pageData);

    cout << "Num Leaf entries after delete: " << leafPageHeader.numEntries << endl;

    cout << "-------------------------------------------------------------------------" << endl;

    if (result == success) {
        cerr << "Let's fucking go" << endl;
        return success;
    } else {
        cerr << "bruh" << endl;
        return fail;
    }
}