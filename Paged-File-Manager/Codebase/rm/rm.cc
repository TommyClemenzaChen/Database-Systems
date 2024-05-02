
#include "rm.h"

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
    if (_rm->tables.empty() || _rm->columns.empty()) {
        return -1;
    }

    void *fileData;
    Table tempTable;
    for (unsigned i = 0; i < _rm->tables.size(); i++) {
        fread(fileData, sizeof(Table), 1, _tD);
        memcpy(&tempTable, (char*)fileData + i * sizeof(Table), sizeof(Table));

        if (tempTable.tableID != _rm->tables[i].tableID
            || tempTable.tableName.compare(_rm->tables[i].tableName) != 0
            || tempTable.fileName.compare(_rm->tables[i].fileName) != 0) {
                
                return -1;
        }
    }

    Column tempCol;
    for (unsigned i = 0; i < _rm->columns.size(); i++) {
        fread(fileData, sizeof(Column), 1, _cD);
        memcpy(&tempCol, (char*)fileData + i * sizeof(Column), sizeof(Column));

        if (tempCol.tableID != _rm->columns[i].tableID
            || tempCol.columnName.compare(_rm->columns[i].columnName) != 0
            || tempCol.columnType != _rm->columns[i].columnType
            || tempCol.columnLength != _rm->columns[i].columnLength
            || tempCol.columnPosition != _rm->columns[i].columnPosition) {

                return -1;
        }
    }


    return 0;
}



RC RelationManager::createCatalog()
{
    //check if config tables exist
    if (validConfigTables() != 0) {
        return -1;
    }

    //Tables
    _rm->tables.push_back({1, "Tables", "Tables"});
    _rm->tables.push_back({2, "Columns", "Columns"});

    //write data to new Tables.tbl file
    _tD = fopen("Tables.tbl", "wb");
    for (int i = 0; i < _rm->tables.size(); i++) {
        fwrite(&tables[i], sizeof(Table), 1, _tD);
    }
    
    //Columns
    _rm->columns.push_back({1, "table-id", TypeInt, 4, 1});
    _rm->columns.push_back({1, "table-name", TypeVarChar, 50, 2});
    _rm->columns.push_back({1, "file-name", TypeVarChar, 50, 3});
    _rm->columns.push_back({2, "table-id", TypeInt, 4, 1});
    _rm->columns.push_back({2, "column-name", TypeVarChar, 50, 2});
    _rm->columns.push_back({2, "column-type", TypeInt, 4, 3});
    _rm->columns.push_back({2, "column-length", TypeInt, 4, 4});
    _rm->columns.push_back({2, "column-position", TypeInt, 4, 5});

    //write data to new Columns.tbl file
    _cD = fopen("Columns.tbl", "wb");
    for (int i = 0; i < _rm->columns.size(); i++) {
        fwrite(&columns[i], sizeof(Column), 1, _cD);
    }


    return validConfigTables();  //should return 0
}

RC RelationManager::deleteCatalog()
{

    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
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



