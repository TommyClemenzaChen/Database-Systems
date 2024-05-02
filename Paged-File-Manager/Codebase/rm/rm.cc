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

//Helper Functions
RC RelationManager::validConfigTables() {

    if (tables.empty() || columns.empty()) {
        cout << "empty";
        return -1;
    }

    FileHandle tablesFH;
    FileHandle columnsFH;
    if (_rbfm->openFile("Tables.tbl", tablesFH) != SUCCESS) {
        return -1;
    }
    if (_rbfm->openFile("Columns.tbl", columnsFH) != SUCCESS) {
        return -1;
    }

    struct stat tsb;
    stat("Tables.tbl", &tsb);
   

    void *tablesData = malloc(tsb.st_size);

    Table tempTable;
    for (unsigned i = 0; i < tables.size(); i++) {
        fread(tablesData, sizeof(Table), 1, _td);
        memcpy(&tempTable, tablesData, sizeof(Table));

        if (tempTable.tableID != tables[i].tableID
            || strcmp(tempTable.tableName, tables[i].tableName) != 0
            || strcmp(tempTable.fileName, tables[i].fileName) != 0) {
                cout << "bruhh1" << endl;
                return -1;
        }
        
        
       
    }
   
    struct stat csb;
    stat("Columns.tbl", &csb);

    void *colData = malloc(csb.st_size);
    

    Column tempCol;
    for (unsigned i = 0; i < columns.size(); i++) {
        fread(colData, sizeof(Column), 1, _cd);
        memcpy(&tempCol, colData, sizeof(Column));

        if (tempCol.tableID != columns[i].tableID
            || strcmp(tempCol.columnName, columns[i].columnName) != 0
            || tempCol.columnType != columns[i].columnType
            || tempCol.columnLength != columns[i].columnLength
            || tempCol.columnPosition != columns[i].columnPosition) {
                cout << "bruhh2" << endl;
                return -1;
        }
    }

    free(tablesData);
    free(colData);
    
    _rbfm->closeFile(tablesFH);
    _rbfm->closeFile(columnsFH);
    

    
    return 0;
}

RC RelationManager::createCatalog()
{

    FileHandle tablesFH;
    if (_rbfm->openFile("Tables.tbl", tablesFH) != SUCCESS) {
        return -1;
    }
    FileHandle columnsFH;
    if (_rbfm->openFile("Columns.tbl", columnsFH) != SUCCESS) {
        return -1;
    }
    
    //Tables
    tables.push_back({1, "Tables", "Tables"});
    tables.push_back({2, "Columns", "Columns"});

    for (const auto& table : tables) {
        fwrite(&table, sizeof(Table), 1, tablesFH->_fd);
    }

    //Columns
    columns.push_back({1, "table-id", TypeInt, 4, 1});
    columns.push_back({1, "table-name", TypeVarChar, 50, 2});
    columns.push_back({1, "file-name", TypeVarChar, 50, 3});
    columns.push_back({2, "table-id", TypeInt, 4, 1});
    columns.push_back({2, "column-name", TypeVarChar, 50, 2});
    columns.push_back({2, "column-type", TypeInt, 4, 3});
    columns.push_back({2, "column-length", TypeInt, 4, 4});
    columns.push_back({2, "column-position", TypeInt, 4, 5});

    //write data to Columns.tbl file
    
    for (const auto& col : columns) {
        fwrite(&col, sizeof(Column), 1, _cd);
    }

    _rbfm->closeFile(tablesFH);
    _rbfm->closeFile(columnsFH);

    return validConfigTables();  //should return 0
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

    Table newTable;
    strcpy(newTable.tableName, tableName.c_str());
    strcpy(newTable.fileName, tableName.c_str());
    newTable.tableID = tables.size() + 1;
    
    fwrite(&newTable, sizeof(Table), 1, _td);

    fflush(_td);

    //create a column for each attribute
    for (int i = 0; i < attrs.size(); i++) {
        Column newColumn;
        strcpy(newColumn.columnName, attrs[i].name.c_str());
        newColumn.tableID = tables.size() + 1;
        newColumn.columnType = attrs[i].type;
        newColumn.columnLength = attrs[i].length;
        newColumn.columnPosition = i + 1;
        
        fwrite(&newColumn, sizeof(Column), 1, _cd);
        fwrite(&newColumn, sizeof(Column), 1, _fd);
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
    struct stat sb;
    if (stat(tableName.c_str(), &sb) != 0) {
        cout << "bruh file doesn't exist" << endl;
        return -1;
    }

    _fd = fopen(tableName.c_str(), "rb+");

    void *tableData = malloc(sb.st_size);
    fread(tableData, 1, sb.st_size, _fd);

    
    Column col;
    for (int i = 0; i < sb.st_size / sizeof(Column); i++) {
        //extract column
        memcpy(&col, (char*)tableData + i * sizeof(Column), sizeof(Column));
    
        //extract attributes
        Attribute attr = {col.columnName, col.columnType, col.columnLength};
        attrs.push_back(attr);
    }
    
    return 0;
    
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{   
    struct stat sb;
    if (stat(tableName.c_str(), &sb) != 0) {
        cout << "bruh file doesn't exist" << endl;
        return -1;
    }


    return -1;
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



