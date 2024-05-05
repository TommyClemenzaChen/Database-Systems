#include "rm.h"
#include <iostream>
#include <sys/stat.h>

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::_rbfm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    //create RBFM instance
    _rbfm = RecordBasedFileManager::instance();
    

}

RelationManager::~RelationManager()
{
}

int RelationManager::getNullIndicatorSize(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

bool RelationManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RelationManager::createTablesDescriptor(vector<Attribute> &tablesDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

}

void RelationManager::createColumnsDescriptor(vector<Attribute> &columnsDescriptor) {
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);
}

RC RelationManager::configureTableData(int fieldCount, unsigned char *nullFieldsIndicator, int tableID, string tableName, string isSystem, void *data) {
    int offset = 0;
    int tablenameLength = tableName.length();
    string fileName = tableName + ".tbl";
    int filenameLength = fileName.length();

    //null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double)fieldCount / 8);

    //null-indicator for the fields
    memcpy((char*)data + offset, &nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;
    
    // Beginning of the actual data    
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]
   
    //Attr: Table-ID
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) {
        memcpy((char*)data + offset, &tableID, sizeof(int));
        offset += sizeof(int);
    }

    //Attr: Table-Name
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char*)data + offset, &tablenameLength, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char*)data + offset, tableName.c_str(), tablenameLength);
        offset += tablenameLength;
    }

    //Attr: File-Name
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char*)data + offset, &filenameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)data + offset, fileName.c_str(), filenameLength);
        offset += filenameLength;
    }

    //Attr: isSystem
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        int systemLength = isSystem.length();
        memcpy((char*)data + offset, &systemLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)data + offset, isSystem.c_str(), systemLength);
        offset += systemLength;
    }

    return 0;
}

RC RelationManager::configureColumnsData(int fieldCount, unsigned char *nullFieldsIndicator, int tableID, 
    string columnName, int columnType, int columnLength, int columnPosition, void *data)
{
    int offset = 0;
    int columnNameLength = columnName.length();

    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getNullIndicatorSize(fieldCount);
    
    memcpy((char*)data + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data    
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

    // Is the name field not-NULL?
    
    //Attr: Table-ID
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) {
        memcpy((char*)data + offset, &tableID, INT_SIZE);
        offset += INT_SIZE;
    }

    //Attr: Column-Name
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        nullBit = nullFieldsIndicator[0] & (1 << 7);
        memcpy((char*)data + offset, &columnNameLength, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char*)data + offset, columnName.c_str(), columnNameLength);
        offset += columnNameLength;
    }
    
    //Attr: Column-Type
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char*)data + offset, &columnType, INT_SIZE);
        offset += INT_SIZE;
    }

    //Attr: Column-Length
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char*)data + offset, &columnLength, INT_SIZE);
        offset += INT_SIZE;
    }

    //Attr: Column-Position
    nullBit = nullFieldsIndicator[0] & (1 << 3);
    if (!nullBit) {
        memcpy((char*)data + offset, &columnPosition, INT_SIZE);
        offset += INT_SIZE;
    }

    return 0;
}
        
RC RelationManager::createCatalog()
{
    //creating catalog tables (Tables and Columns)
    if (_rbfm->createFile("Tables.tbl") != SUCCESS) {
        cout << "File already exists" << endl;
        return -1;
    }

    if (_rbfm->createFile("Columns.tbl") != SUCCESS) {
        cout << "file already exists" << endl;
        return -1;
    }



    RID rid;
    FileHandle fileHandle;
    int nullIndicatorSize;
    unsigned char *nullFieldsIndicator;
    int fieldCount;
    

////////////////////////////////////////////////////////////////////////////////////////////   
    //opening catalog tables
    if (_rbfm->openFile("Tables.tbl", fileHandle) != SUCCESS) {
        cout << "can't open tables" << endl;
        return -1;
    }

    //Catalog "Tables" setup
    vector<Attribute> tablesDescriptor;
    createTablesDescriptor(tablesDescriptor);
    
    nullIndicatorSize = getNullIndicatorSize(tablesDescriptor.size());
    
    nullFieldsIndicator = (unsigned char*)malloc(nullIndicatorSize);
    void *tablesData = malloc(4096); 

    fieldCount = tablesDescriptor.size();

    configureTableData(fieldCount, nullFieldsIndicator, 1, "Tables", "yes", tablesData);
    _rbfm->insertRecord(fileHandle, tablesDescriptor, tablesData, rid);

    configureTableData(fieldCount, nullFieldsIndicator, 2, "Columns", "no", tablesData);
    _rbfm->insertRecord(fileHandle, tablesDescriptor, tablesData, rid);
    
    _rbfm->closeFile(fileHandle);
    free(tablesData);

///////////////////////////////////////////////////////////////////////////////////////////////////
    
    //opening Catalog columns
    if (_rbfm->openFile("Columns.tbl", fileHandle) != SUCCESS) {
        cout << "can't open columns" << endl;
        return -1;
    }

    //Catalog "Columns" setup
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);

    nullIndicatorSize = getNullIndicatorSize(columnsDescriptor.size());
    
    nullFieldsIndicator = (unsigned char*)malloc(nullIndicatorSize);
    void *columnsData = malloc(4096); 

    fieldCount = columnsDescriptor.size();

    configureColumnsData(fieldCount, nullFieldsIndicator, 1, "table-id", TypeInt, 4, 1, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    configureColumnsData(fieldCount, nullFieldsIndicator, 2, "column-name", TypeVarChar, 50, 2, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    configureColumnsData(fieldCount, nullFieldsIndicator, 3, "column-type", TypeInt, 4, 3, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);


    configureColumnsData(fieldCount, nullFieldsIndicator, 4, "column-length", TypeInt, 4, 4, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    
    configureColumnsData(fieldCount, nullFieldsIndicator, 5, "column-position", TypeInt, 4, 5, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);


    
    _rbfm->closeFile(fileHandle);
    free(columnsData);


    return 0;
}

RC RelationManager::deleteCatalog()
{
    remove("Tables.tbl");
    remove("Columns.tbl");
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{   
    RID rid;
    FileHandle fileHandle;
    int nullIndicatorSize;
    unsigned char *nullFieldsIndicator;
    int fieldCount;

    //create the new table file
    if(_rbfm->createFile(tableName + ".tbl") != SUCCESS) {
        return -1;
    }

    //insert new table to catalog "Tables"
    if (_rbfm->openFile("Tables.tbl", fileHandle) != SUCCESS) {
        return -1;
    }

    fieldCount = attrs.size();
    nullIndicatorSize = getNullIndicatorSize(fieldCount);
    nullFieldsIndicator = (unsigned char*)malloc(nullIndicatorSize);

    
    vector<string> attributeNames;
    for (const auto& attr : attrs) {
        attributeNames.push_back(attr.name);
    }

    RBFM_ScanIterator rbfmSI;
    _rbfm->scan(fileHandle, attrs, "", NO_OP, NULL, attributeNames, rbfmSI);

    cout << rbfmSI.totalSlots << endl;

    void *newTableData = malloc(PAGE_SIZE);
    configureTableData(fieldCount, nullFieldsIndicator, rbfmSI.totalSlots, tableName, 0, newTableData);
    _rbfm->insertRecord(fileHandle, attrs, newTableData, rid);

    

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    FileHandle fileHandle;

    //open catalog "Tables" to get table id of tableName
    if (_rbfm->openFile(tableName + ".tbl", fileHandle) != SUCCESS) {
        return 0;
    }


    
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{   
    
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName + ".tbl", fileHandle) != SUCCESS) {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    RBFM_ScanIterator rbfm_ScanIterator;
    /*
    _rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
    rm_ScanIterator = RM_ScanIterator(rbfm_ScanIterator);

    */  
    _rbfm->closeFile(fileHandle);

    return -1;
}