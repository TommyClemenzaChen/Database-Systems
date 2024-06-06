
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {

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

RC Project::getNextTuple(void *data) {
    // Unsure how large each attribute will be, set to size of page to be safe
    void *buffer = malloc(PAGE_SIZE);
    if (buffer == NULL)
        return RBFM_MALLOC_FAILED;
    
    if (_input->getNextTuple(buffer) == QE_EOF)
        return QE_EOF;
    
    unsigned bufOffset;
    unsigned dataOffset;

    // Parse the null indicator into an array
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    int nullIndicatorSize = rbfm->getNullIndicatorSize(_attrs.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    bufOffset += nullIndicatorSize;
    dataOffset += nullIndicatorSize;

    unsigned attrSize;

    for (int i = 0; i < _projectAttrs.size(); i++) {
        for (int j = 0; j < _attrs.size(); i++) {
            switch (_attrs[j].type) {
                case TypeInt:
                    attrSize = INT_SIZE;
                    break;
                case TypeReal:
                    attrSize = REAL_SIZE;
                    break;
                case TypeVarChar:
                    memcpy(&attrSize, (char*)buffer + bufOffset, VARCHAR_LENGTH_SIZE);
                    attrSize += VARCHAR_LENGTH_SIZE;
            }   

            if (_projectAttrs[i].name == _attrs[j].name) {
                break;
            }
            bufOffset += attrSize;
        }
        
        //we found project attribute
        memcpy((char*)data + dataOffset, (char*)buffer + bufOffset, attrSize);
        bufOffset += attrSize;
        dataOffset += attrSize;
    }
    free(buffer);

    return SUCCESS;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs = _projectAttrs;
}

