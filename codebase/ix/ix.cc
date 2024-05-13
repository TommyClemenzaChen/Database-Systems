
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
    InternalPageHeader internalPageHeader;
    internalPageHeader.flag = INTERNAL;
    internalPageHeader.FSO = PAGE_SIZE;
    internalPageHeader.numChildren = 1; //??
    setInternalPageHeader(page, internalPageHeader);
}

RC IndexManager::newMetaPage(void *page) {
    memset(page, 0, PAGE_SIZE);
    //write meta page header
    MetaPageHeader metaPageHeader;
    metaPageHeader.rootNum = 1;
    setMetaPageHeader(page, metaPageHeader);
    
}

void IndexManager::setInternalPageHeader(void *page, InternalPageHeader internalPageHeader) {
    memcpy(page, &internalPageHeader, sizeof(InternalPageHeader));
}

void IndexManager::setMetaPageHeader(void *page, MetaPageHeader metaPageHeader) {
    memcpy(page, &metaPageHeader, sizeof(MetaPageHeader));
}

bool IndexManager::fileExists(const string &fileName) {
    struct stat sb;
    return stat(fileName.c_str(), &sb) == 0;
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

/*????
    void *childPageData = malloc(PAGE_SIZE);
    if (childPageData == NULL) {
        return 5;
    }
*/

    IXFileHandle ixfileHandle;
    ixfileHandle.setfd(iFile);

    ixfileHandle.appendPage(metaPageData);
    ixfileHandle.appendPage(rootPageData);

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

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //should follow B+ tree insertion algorithm
    if (ixfileHandle.getfd() == NULL) {
        return 1;
    }

    for 

    



    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    
    return -1;
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

