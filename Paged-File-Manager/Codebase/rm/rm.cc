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
    _tD = fopen("Tables.tbl", "wb+");
    _cD = fopen("Columns.tbl", "wb+");

    fclose(_tD);
    fclose(_cD);

}

RelationManager::~RelationManager()
{
}

//Helper Functions
RC RelationManager::validConfigTables() {
    rewind(_tD);
    rewind(_cD);

    if (tables.empty() || columns.empty()) {
        cout << "empty";
        return -1;
    }

    struct stat tsb;
    stat("Tables.tbl", &tsb);

    void *tablesData = malloc(tsb.st_size);
    fread(tablesData, 1, tsb.st_size, _tD);

    Table tempTable;
    for (unsigned i = 0; i < tables.size(); i++) {
        
        memcpy(&tempTable, (char*)tablesData + i * sizeof(Table), sizeof(Table));

        if (tempTable.tableID != tables[i].tableID
            || strcmp(tempTable.tableName, tables[i].tableName) != 0
            || strcmp(tempTable.fileName, tables[i].fileName) != 0) {
                
                return -1;
        }
        
        
       
    }
   
    struct stat csb;
    stat("Columns.tbl", &csb);

    void *colData = malloc(csb.st_size);
    fread(colData, 1, csb.st_size, _cD);

    Column tempCol;
    for (unsigned i = 0; i < columns.size(); i++) {
        memcpy(&tempCol, (char*)colData + i * sizeof(Column), sizeof(Column));

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
    
    fclose(_tD);
    fclose(_cD);
    

    
    return 0;
}

RC RelationManager::createCatalog()
{
    
    //Tables
    tables.push_back({1, "Tables", "Tables"});
    tables.push_back({2, "Columns", "Columns"});

    for (const auto& table : tables) {
        fwrite(&table, sizeof(Table), 1, _tD);
    }
    

    fflush(_tD);

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
        fwrite(&col, sizeof(Column), 1, _cD);
    }

    
    fflush(_cD);

    return validConfigTables();  //should return 0
}

RC RelationManager::deleteCatalog()
{

    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{   


    FILE *fd = fopen(tableName.c_str(), "wb+");

    Table newTable;
    strcpy(newTable.tableName, tableName.c_str());
    strcpy(newTable.fileName, tableName.c_str());
    newTable.tableID = tables.size() + 1;
    
    fwrite(&newTable, sizeof(Table), 1, _tD);

    //create a column for each attribute
    for (int i = 0; i < attrs.size(); i++) {
        Column newColumn;
        strcpy(newColumn.columnName, attrs[i].name.c_str());
        newColumn.tableID = tables.size() + 1;
        newColumn.columnType = attrs[i].type;
        newColumn.columnLength = attrs[i].length;
        newColumn.columnPosition = i + 1;
        
        fwrite(&newColumn, sizeof(Column), 1, _cD);
        fwrite(&newColumn, sizeof(Column), 1, fd);
    }

    fflush(_tD);
    fflush(_cD);
    fflush(fd);

    fclose(_tD);
    fclose(_cD);
    fclose(fd);

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    struct stat tsb;
    if (stat(tableName.c_str(), &tsb) != 0) {
        cout << "ended here" << endl;
        return -1;
    }

    FILE *fd = fopen(tableName.c_str(), "rb");

    void *tableData = malloc(tsb.st_size);
    
    
    fread(tableData, 1, tsb.st_size, fd);

    
    Column col;
    

    
        //extract column
        memcpy(&col, (char*)tableData, sizeof(Column));
        cout << col.columnName << " " << col.columnType << " " << col.columnLength << endl;
/*
        //extract attributes
        Attribute attr = {col.columnName, col.columnType, col.columnLength};
    
        
*/
    return 0;
    
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
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



