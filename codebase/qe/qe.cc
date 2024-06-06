#include <cstring>
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    // initialize private vars to what Condition is
    
    _input = input;
    lhsAttr = condition.lhsAttr;
    op = condition.op; // always EQ_OP but ok
    bRhsIsAttr = condition.bRhsIsAttr;
    rhsAttr = condition.rhsAttr;
    rhsValue = condition.rhsValue; // value has AttrType and data!!!!!
    _input.getAttributes(this->attrs);

}

// write a compare function to be used by all classes
bool compare(CompOp compOp, void lhsData, void* rhsValue.data, AttrType type) {
    // assuming that attribute type is the same for both lhs and rhs
	// make a rbfm instance
    RBFM_ScanIterator *rbfm_si = RBFM_ScanIterator::instance();
    switch (type) {
        case TypeReal:
            int32_t recordInt;
            memcpy(&recordInt, (char*)lhsData + 1, sizeof(int));
            return rbfm_si->checkScanCondition(recordInt, compOp, rhsValue.data);
            break;
        case TypeInt:
            float recordReal;
            memcpy(&recordReal, (char*)lhsData + 1, sizeof(float));
            return rbfm_si->checkScanCondition(recordReal, compOp, rhsValue.data);
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, (char*)lhsData + 1, VARCHAR_LENGTH_SIZE);
            char recordString[varcharSize + 1];
            memcpy(recordString, (char*)lhsData + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
            recordString[varcharSize] = '\0';
            return rbfm_si->checkScanCondition(recordString, compOp, rhsValue.data);
            break;
        // won't get here
	    default: return false;
    }
}

// Writing out Utkarsh's pseudocode
RC Filter::getNextTuple(void *data) {
    // input->getNextTuple() ??
    if (_input->getNextTuple(data) == EOF) return QE_EOF;
    do {
        // Fetch the attribute info (do this in Filter)
        // Check the conditions validity
        if (!bRhsIsAttr /* # fields in LHS != 0 || !(data type is the same on both sides) */)
            return BAD_COND;
        // process record
        if (compare(op, lhsAttr.data /*find a way to get the actual value in here - through processRecord*/, rhsValue.data)) 
            return SUCCESS;
    } while (_input->getNextTuple(data));
    
    return -1; // if it doesn't get in it fails
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    // attrs = this->attrs; // how do I get the attributes?
    attrs = this->attrs;
}

// function to process the record data in *data
void processRecord(void *data) 
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // read through record where theres a match, then
        // get value of match and return it
    
    // find out how to get the Attribute vector of lhs so i can:
        // 1) Return the vector in getAttributes
        // 2) get the attribute type
    
    // temporarily use rhsValue.type for this

    void* lhsData;

    // fml i don't have the page lol - will this even work idk what file im even referencing fml
    rbfm->getAttributeFromRecord(page, offset, attrIndex, rhsValue.type, lhsData);

    // can call getAttributes from rm but I don't have a tableName, how do I index through the catalog to find it?


}

