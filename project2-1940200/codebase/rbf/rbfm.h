#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <inttypes.h>
#include "../rbf/pfm.h"

#define INT_SIZE                4
#define REAL_SIZE               4
#define VARCHAR_LENGTH_SIZE     4

#define RBFM_CREATE_FAILED 1
#define RBFM_MALLOC_FAILED 2
#define RBFM_OPEN_FAILED   3
#define RBFM_APPEND_FAILED 4
#define RBFM_READ_FAILED   5
#define RBFM_WRITE_FAILED  6
#define RBFM_SLOT_DN_EXIST 7

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP	   // no condition
} CompOp;

// Slot directory headers for page organization
// See chapter 9.6.2 of the cow book or lecture 3 slide 17 for more information
typedef struct SlotDirectoryHeader
{
    uint16_t freeSpaceOffset;
    uint16_t recordEntriesNumber;
} SlotDirectoryHeader;

typedef struct SlotDirectoryRecordEntry
{
    uint32_t length; 
    int32_t offset;
} SlotDirectoryRecordEntry;

typedef SlotDirectoryRecordEntry* SlotDirectory;

typedef uint16_t ColumnOffset;

typedef uint16_t RecordLength;
/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
  private:
    RecordBasedFileManager* rbfmInstance;
    RC getNextSlot();
    bool isValid(SlotDirectoryRecordEntry recordEntry);
    bool checkCondition();
public:
    RBFM_ScanIterator() : rbfmInstance(RecordBasedFileManager::instance()) {}
    ~RBFM_ScanIterator() {};
    RC getNextRecord(RID &rid, void *data);
    RC close();
};


class RecordBasedFileManager
{
    // Declare RBFM_ScanIterator as a friend class
    // friend class RBFM_ScanIterator;
public:
static RecordBasedFileManager* instance() {
        static RecordBasedFileManager instance;
        return &instance;
    }

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);


  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

 
  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

    

// private:

  // Helper methods
  RC insertRecordOnPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, void *pageData, const RID &rid);

  void newRecordBasedPage(void * page);

  SlotDirectoryHeader getSlotDirectoryHeader(void * page);
  void setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader);

  SlotDirectoryRecordEntry getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber);
  void setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry);

  unsigned getPageFreeSpaceSize(void * page);
  unsigned getRecordSize(const vector<Attribute> &recordDescriptor, const void *data);

  int getNullIndicatorSize(int fieldCount);
  bool fieldIsNull(char *nullIndicator, int i);

  void setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data);
  void getRecordAtOffset(void *record, unsigned offset, const vector<Attribute> &recordDescriptor, void *data);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

};

#endif
