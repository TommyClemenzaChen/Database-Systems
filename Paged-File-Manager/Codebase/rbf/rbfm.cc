#include "rbfm.h"
#include "pfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

// copy constructor
RecordBasedFileManager::RecordBasedFileManager(){}

// destructor
RecordBasedFileManager::~RecordBasedFileManager(){}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // create page here and create slot directory
    
    // store number of records and the free space offset
    return PagedFileManager.createFile(&fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager.destroyFile(&fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager.openFile(&fileName, &fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager.closeFile(&fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}


// helper structs
class Record {
  public:
    static Record* instance(); // Access to the Record instance

    struct rec {
        list<void *> recordList;
        int num_records;
        unsigned null_indicators;
        vector<Attribute> &rd; 
        const void *data;
        RID &rid;
    };
    // initialize Record

    uint32_t calculateRecordSize(rec r) {
        uint32_t size = 0;
        for (int i = 0; i < r.num_records; i++) {
            // Calculate size of each record and add it to the total size
        }
        return size;
    }
    
    void* readNBytes(int charFlag; const void* recData, int n) {
        // charFlag 0 = int/real (4 bytes)
        // charFlag 1 = varchar (4 bytes + strlen)
        
        if (charFlag == 0) {
            void* buffer = new char[n]; // Allocate memory to store the read bytes

            // Copy n bytes from recData into the buffer
            memcpy(buffer, recData, n);

            return buffer;
        }
        else {
            const char* charData = static_cast<const char*>(recData);
            char* buffer = new char[n]; // Allocate memory to store the read bytes

            // Read n bytes
            for (int i = 0; i < n; i++) {
                buffer[i] = charData[i]; // Store the byte in the buffer
            }

            return buffer; // Return the buffer containing the read bytes
        }
    }

    // this will not work as is because:
        // 1) data* has n byte null indicators for y fields that im not accounting for 
        // 2) readNBytes reads from the beginning of the stream each time so it's not 
            // getting the proper data every time its called
    // solution? combine readNBytes with createRecord. splitting them up actually puts 
        // us at a disadvantage. We need access to where data* left off.
    void createRecord(void) {
        // loop through vector to grab all the data
            for (auto i = 0; i < rd.size(); i++) {
                if (rd.at(i).type != TypeVarChar) {
                    // read 4 bytes from data* and then append it to list.
                    list.push(readNBytes(0, data, 4));
                } else {
                    // read 4+strlen bytes from data if the type is char.
                    list.push(readNBytes(1, data, 4+rd.at(i).length))
                }
            }
    }

    

    protected:
        // Constructor
        Record(int num_records, unsigned null_indicators, vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
            num_records = num_records;
            null_indicators = null_indicators;
            vector<Attribute> &rd; 
            const void *data = data;
            RID &rid = rid;
            recordList = {num_records, null_indicators}
            
        };                                   				
        ~Record();
}






