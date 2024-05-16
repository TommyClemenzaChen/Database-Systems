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

    //indexManager->splitLeafPage(data, 1, ixfileHandle, attr);

    return SUCCESS;
}


int main () {
    indexManager = IndexManager::instance();
    
    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

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