#include "qe.h"

// Filter methods
// constructor, initialization
Filter::Filter(Iterator *input, const Condition &condition)
{
    iterator = input;
    cond = &condition;
    iterator->getAttributes(attrs);
    index = getAttrIndex(attrs, cond->lhsAttr);

    compVal = malloc(5 + attrs[index].length);
    attrVal = malloc(5 + attrs[index].length);
}

// destructor, free memory
Filter::~Filter()
{
    free(compVal);
    free(attrVal);
}

RC Filter::getNextTuple(void *data)
{
    while (iterator->getNextTuple(data) != -1)
    {
        getAttrFromTuple(attrs, cond->lhsAttr, data, attrVal);
        *(unsigned char *)compVal = 0;

        if (cond->rhsValue.type == TypeVarChar)
        {
            int len = *(int *)cond->rhsValue.data;
            memcpy((char *)compVal + 1, cond->rhsValue.data, sizeof(int) + len);
        }

        else
            memcpy((char *)compVal + 1, cond->rhsValue.data, sizeof(int));

        if (check_Match(attrs[index].type, attrVal, compVal, cond->op))
        {
            return 0;
        }
    }
    return -1;
}

// Project methods
Project::Project(Iterator *input, const vector<string> &attrNames)
{

    this->attrNames = attrNames;
    this->iterator = input;
    iterator->getAttributes(this->attrs);

    for (size_t i = 0; i < attrNames.size(); i++)
    {
        int index = getAttrIndex(attrs, attrNames[i]);
        projAttrIndexes.push_back(index);
        projAttrs.push_back(attrs[index]);
    }

    tempBuffer = malloc(PAGE_SIZE);
}

RC Project::getNextTuple(void *data)
{
    while (iterator->getNextTuple(tempBuffer) != -1)
    {
        if (getAttrsFromTuple(attrs, projAttrIndexes, tempBuffer, data) == 0)
        {
            return 0;
        }
    }
    return -1;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs = projAttrs;
}

Project::~Project()
{
    free(tempBuffer);
}
