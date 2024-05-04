
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <cmath>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator(RBFM_ScanIterator &rbfm_ScanIterator);
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

private:
  RBFM_ScanIterator rbfm_ScanIterator;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


  //Helper Functions
  RC validConfigTables();
  void createColumnsDescriptor(vector<Attribute> &columnsDescriptor);
  void createTablesDescriptor(vector<Attribute> &tablesDescriptor);
  RC configureTableData(int fieldCount, unsigned char *nullFieldsIndicator, int tableID, string tableName, string isSystem, void *data);
  
  RC configureColumnsData(int fieldCount, unsigned char *nullFieldsIndicator, int tableID, 
    string columnName, int columnType, int columnLength, int columnPosition, void *data);
  
  int getNullIndicatorSize(int fieldCount);
  bool fieldIsNull(char *nullIndicator, int i);


  protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbfm;

  FILE *_td;  //Catalog table -> Tables
  FILE *_cd;  //Catalog table -> Columns
  FILE *_fd;  //Any table file
  
};

#endif