
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};
  RBFM_ScanIterator _rbfmScanItr;
  FileHandle _fh;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close() { return -1; };
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key) {return RM_EOF;};  	// Get next matching entry
  RC close() {return -1;};             			// Terminate index scan
};


int getAttrIndex(const vector<Attribute> &attrs, const string &attrName);
int getAttrFromTuple(const vector<Attribute> &attrs, const string &attrName, const void *data, void *value);
int getAttrsFromTuple(const vector<Attribute> &attrs, const vector<int> projAttrIndexes, const void *data, void *returnedData);
int varchar_cmp(const char *key1, const char *key2);
bool check_Match(AttrType type, const void *value1, const void *value2, const CompOp &compOp);

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

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);
// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

};

#endif
