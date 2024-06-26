#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>

#include "../rbf/rbfm.h"

using namespace std;

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef enum {
    INTERNAL = 0,
    LEAF = 1
} Flag;

typedef struct MetaPageHeader{
    unsigned rootNum;
} MetaPageHeader;

typedef struct TrafficPair {
    void *key;
    PageNum pageNum;
} TrafficPair;

typedef struct LeafPair {
    void *key;
    RID rid;
} Pair;

typedef struct InternalPageHeader {
    Flag flag;
    unsigned FSO;
    unsigned numEntries;
} InternalPageHeader;

typedef struct LeafPageHeader {
    Flag flag;
    unsigned FSO;
    unsigned next;
    unsigned prev;
    unsigned numEntries;

} LeafPageHeader;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        RC insert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        bool fileExists(const string &fileName);

        unsigned getKeyStringLength(void *data);

        int compareKeys(Attribute attr, void *key1, void *key2);

        bool compareRIDS(RID &rid1, RID &rid2);

        bool recordExists(void *pageData, void *key, RID &rid, Attribute &attr);

        RC newInternalPage(void *page);

        RC newMetaPage(void *page);

        RC newLeafPage(void *page);

        void setInternalPageHeader(void *page, InternalPageHeader internalPageHeader);

        void setMetaPageHeader(void *page, MetaPageHeader metaPageHeader);

        void setLeafPageHeader(void *page, LeafPageHeader LeafPageHeader);

        InternalPageHeader getInternalPageHeader(void *page);

        MetaPageHeader getMetaPageHeader(void *page);

        LeafPageHeader getLeafPageHeader(void *page);

        bool isLeafPage(void *page);

        bool isInternalPage(void *page);

        RC splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle ixFileHandle, Attribute attr);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();


    private:
        static IndexManager *_index_manager;
     
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);

    RC writePage(PageNum pageNum, const void *data);

    RC appendPage(const void *data);

    unsigned getNumberOfPages();

    // Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class IndexManager;
    
    private:
       
    FILE *_fd;
    void setfd(FILE *fd);
    FILE* getfd();

};

#endif
