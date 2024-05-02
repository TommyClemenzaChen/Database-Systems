
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

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

RC RelationManager::createCatalog()
{
    //check if config tables exist
    if (validConfigTables()) {
        return 0;
    }

    //Tables
    _rm->tables.push_back({1, "Tables", "Tables"});
    _rm->tables.push_back({2, "Columns", "Columns"});

    //write data to new Tables.tbl file
    FILE *tF = fopen("Tables.tbl", "wb");
    for (int i = 0; i < _rm->tables.size(); i++) {
        fwrite(&tables[i], sizeof(Table), 1, tF);
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
    FILE *cF = fopen("Columns.tbl", "wb");
    for (int i = 0; i < _rm->columns.size(); i++) {
        fwrite(&columns[i], sizeof(Column), 1, cF);
    }
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

//Helper Functions
bool RelationManager::validConfigTables() {
    
}



