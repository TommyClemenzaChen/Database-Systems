#include <algorithm>
#include <iostream>
#include <cstring>
#include "qe.h"

// write a compare function to be used by all classes
bool Iterator::compare(CompOp compOp, void* lhsData, void* data, AttrType type) {
    // assuming that attribute type is the same for both lhs and rhs
	// make a rbfm instance
    RBFM_ScanIterator *rbfm_si = nullptr;
    //NOTE: we do +1 because we want to read pass 1 byte for null indicator
    switch (type) {
        case TypeInt:
            int32_t recordInt;
            memcpy(&recordInt, (char*)lhsData, INT_SIZE);
            //cout << recordInt << endl;
            return rbfm_si->checkScanCondition(recordInt, compOp, data);
            break;
        case TypeReal:
            float recordReal;
            memcpy(&recordReal, (char*)lhsData, sizeof(float));
            //cout << recordReal << endl;
            return rbfm_si->checkScanCondition(recordReal, compOp, data);
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, (char*)lhsData, VARCHAR_LENGTH_SIZE);
            char *recordString = (char*)malloc(varcharSize+1); // if u get malloc error make sure every varchar malloc is +1
            memcpy(recordString, (char*)lhsData + VARCHAR_LENGTH_SIZE, varcharSize);
            recordString[varcharSize] = '\0';
            //cout << recordString << endl;
            return rbfm_si->checkScanCondition(recordString, compOp, data);
            break;
    }
    //should never get here
    return false;
}

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

// Writing out Utkarsh's pseudocode
RC Filter::getNextTuple(void *data) {
    // input->getNextTuple() ??
    if (_input->getNextTuple(data) == EOF)
        return QE_EOF;
    //RelationManager *rm = RelationManager::instance();
    do {
        //rm->printTuple(_attrs, data);
        // Fetch the attribute info (do this in Filter)
        void* lhsData = malloc(PAGE_SIZE);
        processRecord(data, lhsData);
        
        // Check the conditions validity
        if (_bRhsIsAttr) {
            cout << "_bRhsIsAttr" << endl;
            return BAD_COND;
        }
        if (_lhsFieldIsNull) {
            cout << "_lhsFieldIsNull" << endl;
            return BAD_COND;
        }
        if (!_rhsAndLhsTypesMatch) {
            cout << "!_rhsAndLhsTypesMatch" << endl;
            return BAD_COND;
        }

        //now we have left hand side data, but we need to check if it's what we want
        if (_input->compare(_op, lhsData, _rhsValue.data, _rhsValue.type))
            return SUCCESS;
        free(lhsData);

    } while (_input->getNextTuple(data) != QE_EOF);
    
    return -1; // if it doesn't get in it fails
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    // attrs = this->attrs; // how do I get the attributes?
    attrs = _attrs;
}

// function to process the record data in *data
RC Filter::processRecord(void *data, void* lhsData) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    //rbfm->printRecord(_attrs, data);
    // Prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(_attrs.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // Keep track of offset into data
    unsigned offset = nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)_attrs.size(); i++) {
        _lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i);
        _rhsAndLhsTypesMatch = (_attrs[i].type == _rhsValue.type);
        if (_lhsFieldIsNull) {
            continue;
        }
        switch (_attrs[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                memcpy((char*)lhsData, &data_integer, INT_SIZE);
                               
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

                memcpy(lhsData, &varcharSize, VARCHAR_LENGTH_SIZE);
                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;

                memcpy(data_string, ((char*) data + offset), varcharSize);
                offset += varcharSize;

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                            
                memcpy((char*)lhsData + VARCHAR_LENGTH_SIZE, data_string, varcharSize);
            
                free(data_string);
            break;
        }
        if (_attrs[i].name == _lhsAttr) {
            if ((_lhsFieldIsNull = rbfm->fieldIsNull(nullIndicator, i))) {
                return -2; // fix
            }
            if (!(_rhsAndLhsTypesMatch = (_attrs[i].type == _rhsValue.type))) {
                return -3; // fix so there's no magic numbers
            }
            break;
        }
    }
    return SUCCESS;
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
    _input = input;
    //store input attributes in vector
    _attrs.clear();
    _input->getAttributes(_attrs);

    for (int i = 0; i < _attrs.size(); ++i){
		for (int j = 0; j < _attrs.size(); ++j){
			if(attrNames[i] == _attrs[j].name)
				_projectAttrs.push_back(_attrs[j]);
		}

	}
}

RC Project::getNextTuple(void *data) {
    // Unsure how large each attribute will be, set to size of page to be safe
    void *res = malloc(PAGE_SIZE);
    if (res == NULL)
        return RBFM_MALLOC_FAILED;
    
    if (_input->getNextTuple(data) == QE_EOF)
        return QE_EOF;

    // Parse the null indicator into an array
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    int nullIndicatorSize = rbfm->getNullIndicatorSize(_attrs.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    unsigned dataOffset = nullIndicatorSize;
    unsigned resOffset = nullIndicatorSize;

    memcpy(res, nullIndicator, nullIndicatorSize);

    //_projectsAttrs = attributes we want
    //_attrs = attributes we have

    unsigned attrSize = 0;
    for (int i = 0; i < _projectAttrs.size(); i++) {
        dataOffset = nullIndicatorSize;
        if (rbfm->fieldIsNull(nullIndicator, i)) {
            continue;
        }
        for (int j = 0; j < _attrs.size(); j++) {
            
            switch (_attrs[j].type) {
                case TypeInt:
                    attrSize = INT_SIZE;
                    break;

                case TypeReal:
                    attrSize = REAL_SIZE;
                    break;

                case TypeVarChar:
                    int varcharSize = 0;
                    memcpy(&varcharSize, (char*)data + dataOffset, VARCHAR_LENGTH_SIZE);
                    attrSize = VARCHAR_LENGTH_SIZE + varcharSize;
                    break;
            }
            if (_attrs[j].name == _projectAttrs[i].name) {
                break;
            }
            dataOffset += attrSize;
        }
        memcpy((char*)res + resOffset, (char*)data + dataOffset, attrSize);
        resOffset += attrSize; //goes to the next spot in formatted *data

    }
    memcpy(data, res, resOffset);
        
    free(res);
    
    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs = _projectAttrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    _leftIn = leftIn;
    _rightIn = rightIn;
    
    _cond = condition;

    _leftData = malloc(PAGE_SIZE);
    _rightData = malloc(PAGE_SIZE);
    _resultData = malloc(PAGE_SIZE);

    _readLeft = true;
    
    _leftIn->getAttributes(_lhsAttrs);
    _rightIn->getAttributes(_rhsAttrs);

    _resultDataSize = 0;
    _resultOffset = 0;

     // add lhsAttr + rhsAttr 
    for (int i = 0; i < _lhsAttrs.size(); i++) {
        _outputAttrs.push_back(_lhsAttrs[i]);
    }

    for (int i = 0; i < _rhsAttrs.size(); i++) {
        _outputAttrs.push_back(_rhsAttrs[i]);
    }
}

RC INLJoin::compareValuesOnAttr() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    string lhsAttr = _cond.lhsAttr;
    string rhsAttr = _cond.rhsAttr;
    string finalLeftAttr = "";
    string finalRightAttr = "";
    CompOp op = _cond.op;
    
    //leftNullIndicator
    int leftNullIndicatorSize = rbfm->getNullIndicatorSize(_lhsAttrs.size());
    char leftNullIndicator[leftNullIndicatorSize];
    memset(leftNullIndicator, 0, leftNullIndicatorSize);
    memcpy(leftNullIndicator, _resultData, leftNullIndicatorSize);

    //extract value at lhsAttr
    void *leftValue = malloc(PAGE_SIZE);
    unsigned leftOffset = leftNullIndicatorSize;
    AttrType finalLeftType;

    for (int i = 0; i < _lhsAttrs.size(); i++) {
        switch (_lhsAttrs[i].type) {
            case TypeInt:
                memcpy(leftValue, (char*)_leftData + leftOffset, INT_SIZE);
                leftOffset += INT_SIZE;
                finalLeftType = TypeInt;
              

                break;
            case TypeReal:
                memcpy(leftValue, (char*)_leftData + leftOffset, REAL_SIZE);
                leftOffset += REAL_SIZE;
                finalLeftType = TypeReal;
               

                break;

            case TypeVarChar:
                int varcharSize = 0;
                memcpy(&varcharSize, (char*)_leftData + leftOffset, VARCHAR_LENGTH_SIZE);
                memcpy(leftValue, &varcharSize, VARCHAR_LENGTH_SIZE);
                leftOffset += VARCHAR_LENGTH_SIZE;

                memcpy((char*)leftValue + VARCHAR_LENGTH_SIZE, (char*)_leftData + leftOffset, varcharSize);
                leftOffset += varcharSize;
                finalLeftType = TypeVarChar;
                
                
                break;
        }

        if (_lhsAttrs[i].name == lhsAttr) {
            finalLeftAttr = lhsAttr;
            break;
        }
    }

    //rightNullIndicator
    int rightNullIndicatorSize = rbfm->getNullIndicatorSize(_rhsAttrs.size());
    char rightNullIndicator[rightNullIndicatorSize];
    memset(rightNullIndicator, 0, rightNullIndicatorSize);
    memcpy(rightNullIndicator, _resultData, rightNullIndicatorSize);

    //extract value at rhsAttr
    void *rightValue = malloc(PAGE_SIZE);
    unsigned rightOffset = rightNullIndicatorSize;
    AttrType finalRightType;

    for (int i = 0; i < _rhsAttrs.size(); i++) {
        switch (_rhsAttrs[i].type) {
            case TypeInt:
                memcpy(rightValue, (char*)_rightData + rightOffset, INT_SIZE);
                rightOffset += REAL_SIZE;
                finalRightType = TypeInt;
               

                break;
            case TypeReal:
                memcpy(rightValue, (char*)_rightData + rightOffset, REAL_SIZE);
                rightOffset += REAL_SIZE;
                finalRightType = TypeReal;
              

                break;

            case TypeVarChar:
                int varcharSize = 0;
                memcpy(&varcharSize, (char*)_rightData + rightOffset, VARCHAR_LENGTH_SIZE);
                memcpy(rightValue, &varcharSize, VARCHAR_LENGTH_SIZE);
                rightOffset += VARCHAR_LENGTH_SIZE;

                memcpy((char*)rightValue + VARCHAR_LENGTH_SIZE, (char*)_rightData + rightOffset, varcharSize);
                rightOffset += varcharSize;
                finalRightType = TypeVarChar;
                
                break;
        }

        if (_rhsAttrs[i].name == rhsAttr) {
            //save attr type
            finalRightAttr = rhsAttr;
            break;
        }

    }

    //now we have leftValue and rightValue, compare them
    
    return compare(op, leftValue, rightValue, finalLeftType);

}

void INLJoin::setNullIndicatorBit(char *nullIndicator, int i) {
    int indicatorIndex = i / CHAR_BIT;
    char indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] |= indicatorMask;
}

void INLJoin::updateNullIndicator() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    
    //leftNullIndicator
    int leftNullIndicatorSize = rbfm->getNullIndicatorSize(_lhsAttrs.size());
    char leftNullIndicator[leftNullIndicatorSize];
    memset(leftNullIndicator, 0, leftNullIndicatorSize);
    memcpy(leftNullIndicator, _leftData, leftNullIndicatorSize);

    //rightNullIndicator 
    int rightNullIndicatorSize = rbfm->getNullIndicatorSize(_rhsAttrs.size());
    char rightNullIndicator[rightNullIndicatorSize];
    memset(rightNullIndicator, 0, rightNullIndicatorSize);
    memcpy(rightNullIndicator, _rightData, rightNullIndicatorSize);

    //outputNullIndicator
    int outputNullIndicatorSize = rbfm->getNullIndicatorSize(_outputAttrs.size());
    char outputNullIndicator[outputNullIndicatorSize];
    memset(outputNullIndicator, 0, outputNullIndicatorSize);
    memcpy(outputNullIndicator, _resultData, outputNullIndicatorSize);
   

    //add in left null indicator
    for (int i = 0; i < _lhsAttrs.size(); i++) {
        int leftByteIndex = i / CHAR_BIT;
        int leftBitIndex = CHAR_BIT - 1 - (i % CHAR_BIT);
        if (leftNullIndicator[leftByteIndex] & (1 << leftBitIndex)) {
            setNullIndicatorBit(outputNullIndicator, i);
        }
    }

    //add in right null indicator
    for (int i = 0; i < _lhsAttrs.size(); i++) {
        int rightByteIndex = i / CHAR_BIT;
        int rightBitIndex = CHAR_BIT - 1 - (i % CHAR_BIT);
        if (leftNullIndicator[rightByteIndex] & (1 << rightBitIndex)) {
            setNullIndicatorBit(outputNullIndicator, i);
        }
    }

    memcpy(_resultData, outputNullIndicator, outputNullIndicatorSize);

}

RC INLJoin::getNextTuple(void *data) {
    if (_readLeft) {
        buildLeftResult();
        _readLeft = false;
    }
    if (_rightIn->getNextTuple(_rightData) == EOF) {
        _readLeft = true;
        _rightIn->setIterator(NULL, NULL, true, true);
        return getNextTuple(data);
    }
    if (!compareValuesOnAttr()) {
        return getNextTuple(data);
    }

    buildRightResult();
    memcpy(data, _resultData, _resultDataSize);
    return SUCCESS;
}

RC INLJoin::buildRightResult() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    //call update null indicator
    updateNullIndicator();

    rbfm->printRecord(_rhsAttrs, _rightData);

    //Utkarsh logic: need to manipulate the rightNullIndicator bits somehow :)
    //do some form of bit manipulation here
    
    //right tuple values
    unsigned rightOffset = rbfm->getNullIndicatorSize(_rhsAttrs.size());
    for (int i = 0; i < _rhsAttrs.size(); i++) {
        switch (_rhsAttrs[i].type) {
            case TypeInt:
                memcpy((char*)_resultData + _resultOffset, (char*)_rightData + rightOffset, INT_SIZE);
                rightOffset += INT_SIZE;
                _resultOffset += INT_SIZE;
            
                break;
            case TypeReal:
                memcpy((char*)_resultData + _resultOffset, (char*)_rightData + rightOffset, REAL_SIZE);
                rightOffset += REAL_SIZE;
                _resultOffset += REAL_SIZE;

                break;
            case TypeVarChar:
                int varcharSize = 0;
                memcpy(&varcharSize, (char*)_rightData + rightOffset, VARCHAR_LENGTH_SIZE);
                memcpy((char*)_resultData + _resultOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
                rightOffset += VARCHAR_LENGTH_SIZE;
                _resultOffset += VARCHAR_LENGTH_SIZE;

                memcpy((char*)_resultData + _resultOffset, (char*)_rightData + rightOffset, varcharSize);
                rightOffset += varcharSize;
                _resultOffset += varcharSize;


                break;
        }
    }

    //at this point, the _resultData should be this format:
    //| leftNullIndicator(s) | rightNullIndicator(s) | left tuple value(s) | right tuple value(s)

    _resultDataSize = _resultOffset; //final size

    rbfm->printRecord(_outputAttrs, _resultData);

/*
    int temp;
    memcpy(&temp, (char*)_resultData + 14, INT_SIZE);
    cout << temp << endl;
 */

    return SUCCESS;
}

//whatever was passed in as data (tuple), format it, then store in _resultData
RC INLJoin::buildLeftResult() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    _resultDataSize = 0;
    _resultOffset = 0;
    
    //leftData: | leftNullIndicator(s) | left tuple value(s) |
    if (_leftIn->getNextTuple(_leftData) == QE_EOF) {
        return QE_EOF;
    }

    rbfm->printRecord(_lhsAttrs, _leftData);
    
    //call update null indicator
    updateNullIndicator();

    //left tuple values
    _resultOffset = rbfm->getNullIndicatorSize(_outputAttrs.size());
    unsigned leftOffset = rbfm->getNullIndicatorSize(_lhsAttrs.size());

    for (int i = 0; i < _lhsAttrs.size(); i++) {
        switch (_lhsAttrs[i].type) {
            case TypeInt:
                memcpy((char*)_resultData + _resultOffset, (char*)_leftData + leftOffset, INT_SIZE);
                leftOffset += INT_SIZE;
                _resultOffset += INT_SIZE;
            
                break;
            case TypeReal:
                memcpy((char*)_resultData + _resultOffset, (char*)_leftData + leftOffset, REAL_SIZE);
                leftOffset += REAL_SIZE;
                _resultOffset += REAL_SIZE;

                break;
            case TypeVarChar:
                int varcharSize = 0;
                memcpy(&varcharSize, (char*)_leftData + leftOffset, VARCHAR_LENGTH_SIZE);
                memcpy((char*)_resultData + _resultOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
                leftOffset += VARCHAR_LENGTH_SIZE;
                _resultOffset += VARCHAR_LENGTH_SIZE;

                memcpy((char*)_resultData + _resultOffset, (char*)_leftData + leftOffset, varcharSize);
                leftOffset += varcharSize;
                _resultOffset += varcharSize;

                break;
        }
    }

    _resultDataSize = _resultOffset; //temporary size of _resultData since we haven't added _rightData

    //by this point, we should have formatted _resultData to be | leftNullIndicator(s) | rightNullIndicator(s) | left tuple value(s) |
    
    return SUCCESS;
}
        
void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs = _outputAttrs;
}