#ifndef _ix_h_
#define _ix_h_

#include <iostream>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>
#include <cstring>
#include <climits>


#include "../rbf/rbfm.h"

using namespace std;

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef enum {
    NO_SPACE = 1,
    DUPLICATE = 2, 
    SPLIT_ERROR = 3,
    I_GIVE_UP = 4,

    
} ERROR_CODES;

typedef enum {
    INTERNAL = 0,
    LEAF = 1,
    UNKNOWN = 2,
} Flag;

typedef struct MetaPageHeader{
    PageNum rootNum;
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
    PageNum leftChildPage;
} InternalPageHeader;

typedef struct LeafPageHeader {
    Flag flag;
    unsigned FSO;
    PageNum next;
    PageNum prev;
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

        RC insertInternalPair(void *pageData, const Attribute &attr, const void *key, const PageNum pageNum);

        RC insertLeafPair(void *pageData, const Attribute &attr, const void *key, const RID &rid);

        RC insert(IXFileHandle &ixfileHandle, const Attribute &attr, const void *key, const RID &rid, const unsigned pageNum, TrafficPair &trafficPair);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC search(PageNum &pageNum, PageNum &resultPageNum, IXFileHandle &ixfileHandle, Attribute attribute, const void *searchKey);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        bool fileExists(const string &fileName);

        unsigned getKeyStringLength(void *data);

        int compareKeys(Attribute attr, const void *key1, const void *key2) const;

        bool compareRIDS(const RID &rid1, const RID &rid2) const;

        bool leafPairExists(void *pageData, const void *key, const RID &rid, const Attribute &attr);
        
        bool trafficPairExists(void *pageData, const void *key, const PageNum pageNum, const Attribute &attr);

        RC newInternalPage(void *page);

        RC newMetaPage(void *page);

        RC newLeafPage(void *page);

        void setInternalPageHeader(void *page, InternalPageHeader internalPageHeader);

        void setMetaPageHeader(void *page, MetaPageHeader metaPageHeader);

        void setLeafPageHeader(void *page, LeafPageHeader LeafPageHeader);

        InternalPageHeader getInternalPageHeader(void *page) const;

        MetaPageHeader getMetaPageHeader(void *page) const;

        LeafPageHeader getLeafPageHeader(void *page) const;

        unsigned getRootPageNum(IXFileHandle &ixfileHandle) const;

        unsigned getChildPageNum(const void *key, void *pageData, Attribute attr) const;

        Flag getFlag(void *page) const;

        unsigned getKeyLength(const void *key, const Attribute attr) const;

        RC splitLeafPage(void *currLeafData, unsigned currPageNum, IXFileHandle &ixFileHandle, Attribute attr, TrafficPair &trafficPair);

        RC splitInternalPage(void *currInternalData, unsigned currPageNum, IXFileHandle &ixFileHandle, Attribute attr, TrafficPair &trafficPair);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        void printKey(const Attribute &attribute, void *pageData, unsigned offset) const;

        void printRID(const Attribute &attribute, void *pageData, unsigned &offset) const;

        void printInternalKeys(const Attribute &attribute, void *pageData) const;

        // Print the B+ tree in pre-order (in a JSON record format)
        void preorder(IXFileHandle &ixFileHandle, PageNum pageNum, const Attribute &attribute, int depth) const;
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void printLeafPageHeader(LeafPageHeader leafPageHeader);
        void printInternalPageHeader(InternalPageHeader internalPageHeader);
    
    protected:
        IndexManager();
        ~IndexManager();

        

    private:
        static IndexManager *_index_manager;
     
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

        friend class IndexManager;
    private:
        IndexManager *_indexManager;
        
        void *_pageData;

        unsigned currOffset;
        unsigned currPage;
        unsigned totalNumEntries;

        unsigned currEntry;

        unsigned totalPage;

        void* currKey;
        RID currRid;

        Attribute attr;
        IXFileHandle _ixfileHandle;

        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;

        LeafPageHeader leafPageHeader;

        RC scanInit(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive);

        // some check condition functions
        RC getFirstLeafPage(PageNum &pageNum, PageNum &resultPageNum);
};



#endif