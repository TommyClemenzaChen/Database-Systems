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

vector<Attribute> RelationManager::createTablesDescriptor() {
    vector<Attribute> tablesDescriptor;

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "isSystem";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    tablesDescriptor.push_back(attr);

    return tablesDescriptor;
}

vector<Attribute> RelationManager::createColumnsDescriptor() {
    vector<Attribute> columnsDescriptor;

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    columnsDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    columnsDescriptor.push_back(attr);

    return columnsDescriptor;
}

RC RelationManager::setTableData(int tableID, string tableName, int isSystem, void *data) {
    int offset = 0;
    int tablenameLength = tableName.length();
    string fileName = tableName + ".tbl";
    int filenameLength = fileName.length();

    //Attr: Table-ID
    memcpy((char*)data + offset, &tableID, INT_SIZE);
    offset += INT_SIZE;

    //Attr: Table-Name
    memcpy((char*)data + offset, &tablenameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*)data + offset, tableName.c_str(), tablenameLength);
    offset += tablenameLength;

    //Attr: File-Name
    memcpy((char*)data + offset, &filenameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*)data + offset, fileName.c_str(), filenameLength);
    offset += filenameLength;

    //Attr: isSystem
    memcpy((char*)data + offset, &isSystem, INT_SIZE);
    offset += INT_SIZE;

    return 0;
}

RC RelationManager::setColumnsData(int tableID, string columnName, int columnType,
    int columnLength, int columnPosition, void *data)
{
    int offset = 0;
    int columnNameLength = columnName.length();
    //Attr: Table-ID
    memcpy((char*)data + offset, &tableID, INT_SIZE);
    offset += INT_SIZE;

    //Attr: Column-Name
    memcpy((char*)data + offset, &columnNameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*)data + offset, columnName.c_str(), columnNameLength);
    offset += columnNameLength;

    //Attr: Column-Type
    memcpy((char*)data + offset, &columnType, INT_SIZE);
    offset += INT_SIZE;

    //Attr: Column-Length
    memcpy((char*)data + offset, &columnLength, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*)data + offset, &columnPosition, INT_SIZE);
    offset += INT_SIZE;

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
   
    //opening catalog tables
    FileHandle fileHandle;
    if (_rbfm->openFile("Tables.tbl", fileHandle) != SUCCESS) {
        cout << "can't open tables" << endl;
        return -1;
    }

    //Catalog "Tables" setup
    vector<Attribute> tablesDescriptor = createTablesDescriptor();
    
    void *tablesData = (void*)malloc(108);  
    
    setTableData(1, "Tables", 1, tablesData);
    _rbfm->insertRecord(fileHandle, tablesDescriptor, tablesData, rid);

    cout << "inserted tables" << endl;

    setTableData(2, "Columns", 1, tablesData);
    _rbfm->insertRecord(fileHandle, tablesDescriptor, tablesData, rid);

    _rbfm->closeFile(fileHandle);
    free(tablesData);

    return 0;
    
    //opening Catalog columns
    if (_rbfm->openFile("Columns.tbl", fileHandle) != SUCCESS) {
        cout << "can't open columns" << endl;
        return -1;
    }

    void *columnsData = malloc(4096);  
    //Catalog "Columns" setup
    vector<Attribute> columnsDescriptor = createColumnsDescriptor();

    setColumnsData(1, "table-id", TypeInt, 4, 1, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    setColumnsData(2, "column-name", TypeVarChar, 50, 2, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    setColumnsData(3, "column-type", TypeInt, 4, 3, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    setColumnsData(4, "column-length", TypeInt, 4, 4, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid
    );
    setColumnsData(5, "column-position", TypeInt, 4, 5, columnsData);
    _rbfm->insertRecord(fileHandle, columnsDescriptor, columnsData, rid);

    free(columnsData);

    _rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteCatalog()
{

    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{   
    //opening catalog tables table
    FileHandle tablesFH;
    if (_rbfm->openFile("Tables.tbl", tablesFH) != SUCCESS) {
        return -1;
    }

    //opening catalog columns table
    FileHandle columnsFH;
    if (_rbfm->openFile("Columns.tbl", columnsFH) != SUCCESS) {
        return -1;
    }
    
    //Creating and opening a new table file
    if (_rbfm->createFile(tableName) != SUCCESS) {
        return -1;
    }
    FileHandle fileHandle;
    if (_rbfm->openFile(tableName, fileHandle) != SUCCESS) {
        return -1;
    }

    



    _rbfm->closeFile(tablesFH);
    _rbfm->closeFile(columnsFH);
    _rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return 0;
    
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


    return -1;
}






