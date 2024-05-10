
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
RC IndexManager::newInternalPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write page header
    PageHeader ph;
    ph.flag = INTERNAL;
    ph.FSO = PAGE_SIZE;
    ph.numChildren = 1; //??
    setPageHeader(page, ph);
}

RC IndexManager::newMetaDataPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write meta page header
    MetaDataHeader mdh;
    mdh.rootNum = 1;
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

    void *metaPageData = malloc(PAGE_SIZE);
    if (indexPageData == NULL) {
        return 3;
    }
    newMetaDataPage(metaPageData);

    void *rootPageData = malloc(PAGE_SIZE);
    if (rootPageData == NULL) {
        return 4;
    }

    newInternalPage(rootPageData);

/*????
    void *childPageData = malloc(PAGE_SIZE);
    if (childPageData == NULL) {
        return 5;
    }


*/

    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if (ixfileHandle.getfd() != NULL) {
        return 1;
    }

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



    FILE *fd;

    
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FILE *iFile;
    if ()
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

void IXFileHandle::setfd(FILE *fd) {
    _fd = fd;
}

FILE* IXFileHandle::getfd() {
    return _fd;
}

bool IndexManager::fileExists(const string &fileName) {
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
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
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

