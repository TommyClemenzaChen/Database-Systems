
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    // initialize private vars to what Condition is
    
    input = input;
    lhsAttr = condition.lhsAttr;
    op = condition.op; // always EQ_OP but ok
    bRhsIsAttr = condition.bRhsIsAttr;
    rhsAttr = condition.rhsAttr;
    rhsValue = condition.rhsValue; // value has AttrType and data!!!!!

}

// write a compare function to be used by all classes
bool compare(CompOp compOp, void lhsData, void* rhsValue.data, AttrType type) {
    // assuming that attribute type is the same for both lhs and rhs
	
    // switch (type) {
    //     case TypeReal:
    //         float lhsVal;
    //         memcpy (&lhsVal, lhsData, sizeof(float));

    //         float rhsVal;
    //         memcpy(&rhsVal, rhsValue.data, sizeof(float))
    //         switch (compOp)
    //         {
    //             case EQ_OP:  // =
    //                 return lhsVal == rhsVal;
    //             break;
    //             case LT_OP:  // <
    //                 return lhsVal < rhsVal;
    //             break;
    //             case GT_OP:  // >
    //                 return lhsVal > rhsVal;
    //             break;
    //             case LE_OP:  // <=
    //                 return lhsVal <= rhsVal;
    //             break;
    //             case GE_OP:  // >=
    //                 return lhsVal >= rhsVal;
    //             break;
    //             case NE_OP:  // !=
    //                 return lhsVal != rhsVal;
    //             break;
    //             case NO_OP:  // no condition
    //                 return true;
    //             break;
    //         }
    //     case TypeInt:
    //         int lhsVal;
    //         memcpy (&lhsVal, lhsData, sizeof(int));

    //         int rhsVal;
    //         memcpy(&rhsVal, rhsValue.data, sizeof(int))
    //         switch (compOp)
    //         {
    //             case EQ_OP:  // =
    //                 return lhsVal == rhsVal;
    //             break;
    //             case LT_OP:  // <
    //                 return lhsVal < rhsVal;
    //             break;
    //             case GT_OP:  // >
    //                 return lhsVal > rhsVal;
    //             break;
    //             case LE_OP:  // <=
    //                 return lhsVal <= rhsVal;
    //             break;
    //             case GE_OP:  // >=
    //                 return lhsVal >= rhsVal;
    //             break;
    //             case NE_OP:  // !=
    //                 return lhsVal != rhsVal;
    //             break;
    //             case NO_OP:  // no condition
    //                 return true;
    //             break;
    //         }
    //     case TypeVarChar:
    //         // get string length of the record

    //         switch (compOp)
    //         {
    //             case EQ_OP:  // =
    //                 return strcmp(lhsVal, rhsVal) == 0;
    //             break;
    //             case LT_OP:  // <
    //                 return lhsVal < rhsVal;
    //             break;
    //             case GT_OP:  // >
    //                 return lhsVal > rhsVal;
    //             break;
    //             case LE_OP:  // <=
    //                 return lhsVal <= rhsVal;
    //             break;
    //             case GE_OP:  // >=
    //                 return lhsVal >= rhsVal;
    //             break;
    //             case NE_OP:  // !=
    //                 return lhsVal != rhsVal;
    //             break;
    //             case NO_OP:  // no condition
    //                 return true;
    //             break;
    //         }
    //     }   
	// return false;
}

// Writing out Utkarsh's pseudocode
RC Filter::getNextTuple(void *data) {
    // input->getNextTuple() ??
    if (input->getNextTuple(data) == EOF) return QE_EOF;
    do {
        // Fetch the attribute info (do this in Filter)
        // Check the conditions validity
        if (!bRhsIsAttr /* # fields in LHS != 0 || !(data type is the same on both sides) */)
            return BAD_COND;
        if (compare(op, lhsAttr, rhsValue.data)) 
            return SUCCESS;
    } while (input->getNextTuple(data));
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs = input->attrs; // how do I get the attributes?
}

// function to process the record data in *data
{


}

