
#include "qe.h"
#include <algorithm>    // std::max

Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	input->getAttributes(this->attrs);
	leftValue = malloc(PAGE_SIZE);
	rightValue = malloc(PAGE_SIZE);
}

// ... the rest of your implementations go here


Project::Project(Iterator *input,			// Iterator of input R
		const vector<string> &attrNames){	//vector containing attribute names
	this->input = input;
	this->needAttrNames = attrNames;
	input->getAttributes(allAttrs);
	getAttributes(this->needAttrs);
	recordData = malloc(PAGE_SIZE);
}

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        ){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	leftValue = malloc(PAGE_SIZE);
	rightValue = malloc(PAGE_SIZE);
	leftRecord = malloc(PAGE_SIZE);
	rightRecord = malloc(PAGE_SIZE);
	isAfterFirstTimeLeft = false;
}



INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
	this->condition = condition;
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->leftRecord = malloc(PAGE_SIZE);
	this->leftValue = malloc(PAGE_SIZE);
	this->rightRecord = malloc(PAGE_SIZE);
	this->rightValue = malloc(PAGE_SIZE);
	this->isAfterFirstTimeLeft = false;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
};

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
	this->input = input;
	this->aggAttr = aggAttr;
	this->op = op;
	input->getAttributes(allAttrs);
	aggValue = malloc(PAGE_SIZE);

	recordData = malloc(PAGE_SIZE);
	memset(aggValue, 0, PAGE_SIZE);
	this->isFind = false;
	this->count = 0;
}
Aggregate::~Aggregate(){
	free(aggValue);
	free(recordData);
}

void Aggregate::calculate(void* value, void* data){
	/*
	 * typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;
	 * */

	int int_value = 0;
	float float_value = 0, agg_floatValue = 0;
	string string_value;

	memcpy(&agg_floatValue, aggValue, sizeof(int));

	if(aggAttr.type == TypeInt){
		memcpy(&int_value, value, sizeof(int));
		float_value = (float)int_value;
	}else if(aggAttr.type == TypeReal){
		memcpy(&float_value, value, sizeof(float));
	}else if(aggAttr.type == TypeVarChar){
		//should not in
	}

	if(this->op == MIN){
		agg_floatValue = min(agg_floatValue, float_value);
		//write back value
		memcpy(aggValue, &agg_floatValue, sizeof(float));
		memcpy(((char*)data) + 1, &agg_floatValue, sizeof(float));
	}else if(this->op == MAX){
		agg_floatValue = max(agg_floatValue, float_value);
		//write back value
		memcpy(aggValue, &agg_floatValue, sizeof(float));
		memcpy(((char*)data) + 1, &agg_floatValue, sizeof(float));
	}else if(this->op == COUNT){
		count++;
		agg_floatValue = count;
		memcpy(aggValue, &agg_floatValue, sizeof(float));
		memcpy(((char*)data) + 1, &agg_floatValue, sizeof(float));
	}else if(this->op == SUM){
		agg_floatValue = agg_floatValue + float_value;
		memcpy(aggValue, &agg_floatValue, sizeof(float));
		memcpy(((char*)data) + 1, &agg_floatValue, sizeof(float));
	}else if(this->op == AVG){
		count++;
		//agg_floatValue only keep adding, without div
		agg_floatValue = agg_floatValue + float_value;
		float avg = agg_floatValue / count;

		//write back value
		memcpy(aggValue, &agg_floatValue, sizeof(float));
		memcpy(((char*)data) + 1, &avg, sizeof(float));
	}
}

RC Aggregate::getNextTuple(void *data){
	while(input->getNextTuple(recordData) != QE_EOF && !isFind){
		int recordOffset = 0;

		recordOffset += ceil((double) allAttrs.size() / CHAR_BIT);

		for(int i = 0; i < allAttrs.size(); i++){
			//find
			if(allAttrs[i].name == aggAttr.name){
				void* value = malloc(PAGE_SIZE);
				if(allAttrs[i].type == TypeVarChar){
					// would not in
					int length;
					memcpy(&length, ((char*)recordData) + recordOffset, sizeof(int));
					recordOffset += sizeof(int);

					memcpy(value, ((char*)recordData) + recordOffset - 4, length + 4); // (int)(varchar)
					recordOffset += sizeof(length);
				}else{
					memcpy(value, ((char*)recordData) + recordOffset, 4);
					recordOffset += 4;
				}
				calculate(value, data);
				free(value);
			}
			if(allAttrs[i].type == TypeVarChar){
				int length;
				memcpy(&length, ((char*)recordData) + recordOffset, sizeof(int));
				recordOffset += sizeof(int);
				recordOffset += length;
			}else{//float or int
				recordOffset += 4;
			}
		}
	}
	if(isFind)
		return QE_EOF;
	else{
		isFind = true;
		return 0;
	}
};

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(int i = 0; i < allAttrs.size(); i++){
		attrs.push_back(allAttrs[i]);
	}
};






