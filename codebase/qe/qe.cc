#include <cstring>
#include <iostream>     // std::cout
#include <algorithm>    // std::find_if
#include <vector>  
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {  
    _input = input;
    _lhsAttr = condition.lhsAttr;
    _op = condition.op; 
    _bRhsIsAttr = condition.bRhsIsAttr;
    _rhsAttr = condition.rhsAttr;
    _rhsValue = condition.rhsValue; // value has AttrType and data!!!!!
    _input->getAttributes(_attrs);

    _lhsFieldIsNull = false;
    _rhsAndLhsTypesMatch = false;
}

// write a compare function to be used by all classes
bool Iterator::compare(CompOp compOp, void* lhsData, void* data, AttrType type) {
    // assuming that attribute type is the same for both lhs and rhs
	// make a rbfm instance
    cout << "In compare, ";
    RBFM_ScanIterator *rbfm_si;

    switch (type) {
        case TypeInt:
            int32_t recordInt;
            memcpy(&recordInt, (char*)lhsData+1, sizeof(int));
            cout << "value: " << recordInt << endl;
            return rbfm_si->checkScanCondition(recordInt, compOp, data);
            break;
        case TypeReal:
            float recordReal;
            memcpy(&recordReal, (char*)lhsData + 1, sizeof(float));
            return rbfm_si->checkScanCondition(recordReal, compOp, data);
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, (char*)lhsData + 1, VARCHAR_LENGTH_SIZE);
            char recordString[varcharSize + 1];
            memcpy(recordString, (char*)lhsData + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
            recordString[varcharSize] = '\0';
            return rbfm_si->checkScanCondition(recordString, compOp, data);
            break;
    }
    return false;
}

// Writing out Utkarsh's pseudocode
RC Filter::getNextTuple(void *data) {
    // input->getNextTuple() ??
    if (_input->getNextTuple(data) == EOF) return QE_EOF;
    do {
        // Process record
        void* lhsData;
        processRecord(data, lhsData);

        // Check the conditions validity
        if (_bRhsIsAttr || _lhsFieldIsNull || !_rhsAndLhsTypesMatch)
            return BAD_COND;
        
        if (_input->compare(_op, lhsData, _rhsValue.data, _rhsValue.type)) 
            return SUCCESS;

    } while (_input->getNextTuple(data));
    
    return -1; // if it doesn't get in it fails
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    // attrs = this->attrs; // how do I get the attributes?
    attrs = this->_attrs;
}

// function to process the record data in *data
RC Filter::processRecord(void *data, void* lhsData) {
    /*RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(attrs.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Unsure how large each attribute will be, set to size of page to be safe
    lhsData = malloc(PAGE_SIZE);
    if (lhsData == NULL) return RBFM_MALLOC_FAILED;
    
    // Keep track of offset into data
    unsigned offset = nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)attrs.size(); i++) {
        

        lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i);
        rhsAndLhsTypesMatch = (attrs[i].type == rhsValue.type);
    
        switch (attrs[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                memcpy(&lhsData, &data_integer, INT_SIZE);
                
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                memcpy(&lhsData, &data_real, REAL_SIZE);
                
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;

                memcpy(data_string, ((char*) data + offset), varcharSize);
                offset += varcharSize;

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                            
                memcpy(lhsData, &data_string, varcharSize);

                free(data_string);
            break;
        }
        if (attrs[i].name == lhsAttr) {
            if ((lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i))) {
                return -2; // fix
            }
            if (!(rhsAndLhsTypesMatch = (attrs[i].type == rhsValue.type))) {
                return -3; // fix so there's no magic numbers
            }
            break;
        }
    }
    return SUCCESS;*/

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(_attrs.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // Unsure how large each attribute will be, set to size of page to be safe
    lhsData = malloc(PAGE_SIZE);
    if (lhsData == NULL) return RBFM_MALLOC_FAILED;
    

    // Keep track of offset into data
    unsigned offset = nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)_attrs.size(); i++) {
        _lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i);
        _rhsAndLhsTypesMatch = (_attrs[i].type == _rhsValue.type);

        if ((_lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i))) {
            cout << "processing a _lhsFieldIsNull" << endl;
            continue;
        }
        if (!_rhsAndLhsTypesMatch) {
            cout << "processing !_rhsAndLhsTypesMatch" << endl;
            continue;
        }

        switch (_attrs[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;
                memcpy(lhsData, &data_integer, INT_SIZE);
                
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                memcpy(lhsData, &data_real, REAL_SIZE);
                
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;

                memcpy(data_string, ((char*) data + offset), varcharSize);
                offset += varcharSize;

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                            
                memcpy(lhsData, &data_string, varcharSize);

                free(data_string);
            break;
        }

        if (_attrs[i].name == _lhsAttr) {
            break;
        }  
    }
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
    _input = input;
    //store input attributes in vector
    _attrs.clear();
    _input->getAttributes(_attrs);

    for (const auto &attrName : attrNames) {
        auto it = find_if(_attrs.begin(), _attrs.end(), [&attrName](const Attribute &attr) {
            return attr.name == attrName;
        });

        //we found something
        if (it != _attrs.end()) {
            _projectAttrs.push_back(*it);
        }
    }
}

RC Project::getNextTuple(void *data) { }
//     // Unsure how large each attribute will be, set to size of page to be safe
//     void *buffer = malloc(PAGE_SIZE);
//     if (buffer == NULL)
//         return RBFM_MALLOC_FAILED;
    
//     if (_input->getNextTuple(buffer) == QE_EOF)
//         return QE_EOF;
    
//     unsigned bufOffset;
//     unsigned dataOffset;

//     // Parse the null indicator into an array
//     RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
//     int nullIndicatorSize = rbfm->getNullIndicatorSize(_attrs.size());
//     char nullIndicator[nullIndicatorSize];
//     memset(nullIndicator, 0, nullIndicatorSize);
//     memcpy(nullIndicator, data, nullIndicatorSize);

//     bufOffset += nullIndicatorSize;
//     dataOffset += nullIndicatorSize;

//     unsigned attrSize;

//     for (int i = 0; i < _projectAttrs.size(); i++) {
//         for (int j = 0; j < _attrs.size(); i++) {
//             switch (_attrs[j].type) {
//                 case TypeInt:
//                     attrSize = INT_SIZE;
//                     break;
//                 case TypeReal:
//                     attrSize = REAL_SIZE;
//                     break;
//                 case TypeVarChar:
//                     memcpy(&attrSize, (char*)buffer + bufOffset, VARCHAR_LENGTH_SIZE);
//                     attrSize += VARCHAR_LENGTH_SIZE;
//             }   

//             if (_projectAttrs[i].name == _attrs[j].name) {
//                 break;
//             }
//             bufOffset += attrSize;
//         }
        
//         //we found project attribute
//         memcpy((char*)data + dataOffset, (char*)buffer + bufOffset, attrSize);
//         bufOffset += attrSize;
//         dataOffset += attrSize;
//     }
//     free(buffer);

//     return SUCCESS;
// }

void Project::getAttributes(vector<Attribute> &attrs) const {}
//     attrs = _projectAttrs;
// }


INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    _leftIn = leftIn;
    _rightIn = rightIn;
    _cond = condition;
    _leftIn->getAttributes(_leftAttr);
    _rightIn->getAttributes(_rightAttr);
    _leftData = malloc(PAGE_SIZE);
    _rightData = malloc(PAGE_SIZE);
    _resultData = malloc(PAGE_SIZE);
    initializeOutputAttr(_leftAttr, _rightAttr, _outputAttr);
    _readLeft = true;
}

// loop to initialize outputAttributes
void INLJoin::initializeOutputAttr(const vector<Attribute> lhsAttr, const vector<Attribute> rhsAttr, vector<Attribute> &outputAttr) {
    // add lhsAttr + rhsAttr 
    for (size_t i = 0; i < lhsAttr.size(); i++) {
        outputAttr.push_back(lhsAttr[i]);        
    }

    for (size_t i = 0; i < rhsAttr.size(); i++) {
        outputAttr.push_back(rhsAttr[i]);        
    }
}

RC INLJoin::getNextTuple(void *data) {
    if (_readLeft) {
        RC rc = _leftIn->getNextTuple(_leftData);
        if (rc) return GET_NEXT_TUPLE_ERROR;
        _readLeft = false;
        buildLeft(_resultData, _leftData); // I don't think this should be resData but TBD
    } 
    RC rc = _rightIn->getNextTuple(_rightData);
    if (rc == QE_EOF) {
        _readLeft = true;
        _rightIn->setIterator(NULL, NULL, true, true); // rightReset
        return getNextTuple(data); 
    }
    
    // Utkarsh said this but idk how to implement, i imagine something like calling prepareRecord?
        // build or fetch the condition attribute from rightData
        // fetch the "name" from the big record (_resultData i think) and left
    if (!compare(EQ_OP, _leftData, _rightData, _cond.rhsValue.type)) {
        return getNextTuple(data);
    }
    // write the attributes in right data to the built data
    buildLeft(_resultData, _rightData);
}

void INLJoin::buildLeft(void *resultData, void *data) {
    // sigh

}
        
void INLJoin::getAttributes(vector<Attribute> &attrs) {
    attrs = _outputAttr;
}