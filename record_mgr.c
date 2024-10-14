#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct RManager
{
	BM_PageHandle bm_pageHandle;	
	BM_BufferPool bm_bufferPool;
	RID rid;
	Expr *exprCond;
	int noOfTuples;
	int firstFreePage;
	int noOfCount;
} RManager;

const int MAX_PAGE_COUNT = 100;
const int TOTAL_ATTRIBUTE = 15; 

RManager *rManager;


extern RC initRecordManager (void *mgmtData)
{
	initStorageManager();
	return RC_OK;
}

extern RC shutdownRecordManager ()
{
	rManager = NULL;
	free(rManager);
	return RC_OK;
}

RC initPageFile(char *name, SM_FileHandle *fileHandle) {
    RC result;
    if ((result = createPageFile(name)) != RC_OK)
        return result;
    if ((result = openPageFile(name, fileHandle)) != RC_OK)
        return result;
    return RC_OK;
}

// Function to write the schema to the first location of the page file
RC RM_writePageFile(SM_FileHandle *fileHandle, char *tableData) {
    RC result;
    if ((result = writeBlock(0, fileHandle, tableData)) != RC_OK)
        return result;
    return RC_OK;
}

//fetch empty slot
int fetchEmptySlots(char *recordData, int totalRecordLength)
{
    int slotIndex = 0;
    int totalSlots = PAGE_SIZE / totalRecordLength; 

    while (slotIndex < totalSlots) {
        if (recordData[slotIndex * totalRecordLength] != '+')
            return slotIndex;
        slotIndex++;
    }
    return -1;
}


// Function to close the page file after writing
RC RM_closeFile(SM_FileHandle *fileHandle) {
    RC result;
    if ((result = closePageFile(fileHandle)) != RC_OK)
        return result;
    return RC_OK;
}

extern RC createTable (char *name, Schema *schema) {
    rManager = (RManager*) malloc(sizeof(RManager));
    initBufferPool(&rManager->bm_bufferPool, name, MAX_PAGE_COUNT, RS_LRU, NULL);

    char tableData[PAGE_SIZE]; 
    char *tableDataHandle = tableData; 
    int returnValue, attributeIndex;

    *(int*)tableDataHandle = 0; 
    tableDataHandle = tableDataHandle + sizeof(int);
    *(int*)tableDataHandle = 1;
    tableDataHandle = tableDataHandle + sizeof(int);
    *(int*)tableDataHandle = schema->numAttr;
    tableDataHandle = tableDataHandle + sizeof(int); 
    *(int*)tableDataHandle = schema->keySize;
    tableDataHandle = tableDataHandle + sizeof(int);
    
    attributeIndex = 0;
    while (attributeIndex < schema->numAttr) {
        strncpy(tableDataHandle, schema->attrNames[attributeIndex], TOTAL_ATTRIBUTE);
        tableDataHandle = tableDataHandle + TOTAL_ATTRIBUTE;
        *(int*)tableDataHandle = (int)schema->dataTypes[attributeIndex];
        tableDataHandle = tableDataHandle + sizeof(int);
        *(int*)tableDataHandle = (int) schema->typeLength[attributeIndex];
        tableDataHandle = tableDataHandle + sizeof(int);
        attributeIndex++;
    }

    SM_FileHandle bm_bufferpool_file_handler;
	
    if ((returnValue = initPageFile(name, &bm_bufferpool_file_handler)) != RC_OK)
        return returnValue;
    if ((returnValue = RM_writePageFile(&bm_bufferpool_file_handler, tableData)) != RC_OK)
        return returnValue;
    if ((returnValue = RM_closeFile(&bm_bufferpool_file_handler)) != RC_OK)
        return returnValue;

    return RC_OK;
}



void initSchema(Schema *tableSchema, SM_PageHandle bm_pageHandle, int numberOfAttributes) {
    int iterator = 0;

    tableSchema->numAttr = numberOfAttributes;
    tableSchema->attrNames = (char**) malloc(sizeof(char*) * numberOfAttributes);
    tableSchema->dataTypes = (DataType*) malloc(sizeof(DataType) * numberOfAttributes);
    tableSchema->typeLength = (int*) malloc(sizeof(int) * numberOfAttributes);

    while (iterator < numberOfAttributes) {
        tableSchema->attrNames[iterator] = (char*) malloc(TOTAL_ATTRIBUTE);
        strncpy(tableSchema->attrNames[iterator], bm_pageHandle, TOTAL_ATTRIBUTE);
        bm_pageHandle = bm_pageHandle + TOTAL_ATTRIBUTE;
       
        tableSchema->dataTypes[iterator] = *(int*) bm_pageHandle;
        bm_pageHandle = bm_pageHandle + sizeof(int);

        tableSchema->typeLength[iterator] = *(int*)bm_pageHandle;
        bm_pageHandle = bm_pageHandle + sizeof(int);

        iterator++;
    }
}


// This function opens the table with the specified table name "tableName" and sets up its schema.
RC openTable (RM_TableData *relation, char *tableName)
{
    SM_PageHandle bm_pageHandle;
    int numberOfAttributes;

    relation->mgmtData = rManager;
    relation->name = tableName;
    
    pinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle, 0);
    
    bm_pageHandle = (char*) rManager->bm_pageHandle.data;
    
    rManager->noOfTuples = *(int*)bm_pageHandle;
    bm_pageHandle = bm_pageHandle + sizeof(int);

    rManager->firstFreePage = *(int*) bm_pageHandle;
    bm_pageHandle = bm_pageHandle + sizeof(int);
    
    numberOfAttributes = *(int*)bm_pageHandle;
    bm_pageHandle = bm_pageHandle + sizeof(int);
    
    Schema *tableSchema;

    tableSchema = (Schema*) malloc(sizeof(Schema));
    
    initSchema(tableSchema, bm_pageHandle, numberOfAttributes);
    
    relation->schema = tableSchema;  

    unpinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    forcePage(&rManager->bm_bufferPool, &rManager->bm_pageHandle);

    return RC_OK;
}
 
// This function returns the number of tuples (records) in the table referenced by "rel"
extern int getNumTuples (RM_TableData *relationTable)
{
	RManager *rManager = relationTable->mgmtData;
	return rManager->noOfTuples;
}
  
extern RC closeTable (RM_TableData *relationTable)
{
	RManager *rManager = relationTable->mgmtData;
	shutdownBufferPool(&rManager->bm_bufferPool);
	return RC_OK;
}

// Pin a page and return its data
char* pinAndgetPageData(BM_BufferPool *bm_bufferPool, BM_PageHandle *bm_pageHandle, PageNumber pageNum) {
    pinPage(bm_bufferPool, bm_pageHandle, pageNum);
    return bm_pageHandle->data;
}

// This function deletes the table having table name "name"
extern RC deleteTable (char *pageFileName)
{
	destroyPageFile(pageFileName);
	return RC_OK;
}

// Find a free slot for the record in the table
int findEmptySlot(RManager *manager, int recordSize) {
    char *data = manager->bm_pageHandle.data;
    return fetchEmptySlots(data, recordSize);
}

// Insert a new record into the table
extern RC insertRecord (RM_TableData *relation, Record *record)
{
    RManager *tableRecordManager = relation->mgmtData;    
    RID *newRecordID = &record->id; 
    char *pageData, *recordDataPointer;
    int totalRecordLength = getRecordSize(relation->schema);
    
    newRecordID->page = tableRecordManager->firstFreePage;
    pageData = pinAndgetPageData(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle, newRecordID->page);
    newRecordID->slot = findEmptySlot(tableRecordManager, totalRecordLength);
    
    while(newRecordID->slot == -1)
    {
        unpinPage(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle);    
        newRecordID->page++;
        pageData = pinAndgetPageData(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle, newRecordID->page);
        newRecordID->slot = findEmptySlot(tableRecordManager, totalRecordLength);
    }
    
    recordDataPointer = pageData;
    markDirty(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle);
    recordDataPointer = recordDataPointer + (newRecordID->slot * totalRecordLength);
    *recordDataPointer = '+';
    memcpy(++recordDataPointer, record->data + 1, totalRecordLength - 1);
    unpinPage(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle);
    tableRecordManager->noOfTuples++;
    pinPage(&tableRecordManager->bm_bufferPool, &tableRecordManager->bm_pageHandle, 0);
    
    return RC_OK;
}


// Update the free page in the Record Manager
void updateEmptyPage(RManager *manager, PageNumber pageNum) {
    manager->firstFreePage = pageNum;
}

// Delete a record from the table
extern RC deleteRecord (RM_TableData *rel, RID id)
{
    RManager *rManager = rel->mgmtData;
    pinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle, id.page);
    updateEmptyPage(rManager, id.page);
    char *page_data = pinAndgetPageData(&rManager->bm_bufferPool, &rManager->bm_pageHandle, id.page);
    int totalRecordLength = getRecordSize(rel->schema);
    int slotRecordOffset = id.slot * totalRecordLength;
    page_data = page_data + slotRecordOffset;
    char placeholder = '-';
    *page_data = placeholder;
    markDirty(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    unpinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    return RC_OK;
}


// Update a record from the table
extern RC updateRecord (RM_TableData *rel, Record *fetched_record)
{   
    RManager *rManager = rel->mgmtData;
    pinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle, fetched_record->id.page);
    char *bufferPool_page_data;
    int totalRecordLength = getRecordSize(rel->schema);
    RID fetched_record_id = fetched_record->id;
    int slotRecordOffset = fetched_record_id.slot * totalRecordLength;
    bufferPool_page_data = pinAndgetPageData(&rManager->bm_bufferPool, &rManager->bm_pageHandle, fetched_record_id.page);
    bufferPool_page_data = bufferPool_page_data + slotRecordOffset;
    char placeholder = '+';
    *bufferPool_page_data = placeholder;
    memcpy(++bufferPool_page_data, fetched_record->data + 1, totalRecordLength - 1 );
    markDirty(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    unpinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    return RC_OK;    
}


// Retrieve a record from the table
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RManager *rManager = rel->mgmtData;
    pinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle, id.page);
    int totalRecordLength = getRecordSize(rel->schema);
    int slotRecordOffset = id.slot * totalRecordLength;
    char *page_data = pinAndgetPageData(&rManager->bm_bufferPool, &rManager->bm_pageHandle, id.page);
    page_data = page_data + slotRecordOffset;
    char placeholder = '+';
    if(*page_data != placeholder)
    {
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }
    else
    {
        record->id = id;
        char *data = record->data;
        memcpy(++data, page_data + 1, totalRecordLength - 1);
    }
    unpinPage(&rManager->bm_bufferPool, &rManager->bm_pageHandle);
    return RC_OK;
}


// Open the table in memory
void openTableWithScanTable(RM_TableData *rel) {
    openTable(rel, "ScanTable");
}

// Allocate memory for scan manager and initialize scan parameters
void initScanManager(RM_ScanHandle *scan, Expr *cond, RM_TableData *rel, RManager *tableManager) {
    RManager *scanManager = (RManager*) malloc(sizeof(RManager));
    scan->mgmtData = scanManager;
    scanManager->rid.page = 1;
    scanManager->rid.slot = 0;
    scanManager->noOfCount = 0;
    scanManager->exprCond = cond;
    scan->rel = rel;
    tableManager->noOfTuples = TOTAL_ATTRIBUTE;
}

// Start scanning all records using the specified exprCond
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if (cond == NULL)
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    openTableWithScanTable(rel);
    RManager *tableManager = rel->mgmtData;
    initScanManager(scan, cond, rel, tableManager);
    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *fetched_record) {
    RManager *scan_manager = scan->mgmtData;
    RManager *table_manager = scan->rel->mgmtData;
    Schema *table_schema = scan->rel->schema;

    if (scan_manager->exprCond == NULL) {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    Value *expr_result = (Value *) malloc(sizeof(Value));
    char *data;
    int noOfRecords = getRecordSize(table_schema);
    int noOfSlots = PAGE_SIZE / noOfRecords;
    int tuples_count = table_manager->noOfTuples;

    if (tuples_count == 0) {
        return RC_RM_NO_MORE_TUPLES;
    }

    for (int scan_count = scan_manager->noOfCount; scan_count <= tuples_count; scan_count++) {
        if (scan_count <= 0) {
            scan_manager->rid.page = 1;
            scan_manager->rid.slot = 0;
        } else {
            scan_manager->rid.slot++;
            if(scan_manager->rid.slot >= noOfSlots) {
                scan_manager->rid.slot = 0;
                scan_manager->rid.page++;
            }
        }

        pinPage(&table_manager->bm_bufferPool, &scan_manager->bm_pageHandle, scan_manager->rid.page);
        data = scan_manager->bm_pageHandle.data;
        data = data + (scan_manager->rid.slot * noOfRecords);

        fetched_record->id.page = scan_manager->rid.page;
        fetched_record->id.slot = scan_manager->rid.slot;

        char *records_data_pointer = fetched_record->data;
        *records_data_pointer = '-';
        memcpy(++records_data_pointer, data + 1, noOfRecords - 1);

        scan_manager->noOfCount++;

        evalExpr(fetched_record, table_schema, scan_manager->exprCond, &expr_result); 

        if(expr_result->v.boolV == TRUE) {
            unpinPage(&table_manager->bm_bufferPool, &scan_manager->bm_pageHandle);
            return RC_OK;
        }
    }

    unpinPage(&table_manager->bm_bufferPool, &scan_manager->bm_pageHandle);
    scan_manager->rid.page = 1;
    scan_manager->rid.slot = 0;
    scan_manager->noOfCount = 0;

    return RC_RM_NO_MORE_TUPLES;
}



// This function closes the scan operation.
extern RC closeScan(RM_ScanHandle *scan)
{
    RManager *scan_manager = scan->mgmtData;
    RManager *record_manager = scan->rel->mgmtData;

    if(scan_manager->noOfCount < 0) {
        printf("Error: Negative value");
    }

    if(scan_manager->noOfCount > 0)
    {
        unpinPage(&record_manager->bm_bufferPool, &scan_manager->bm_pageHandle);
        
        scan_manager->noOfCount = 0;
        scan_manager->rid.page = 1;
        scan_manager->rid.slot = 0;
    }
    
    scan->mgmtData = NULL;
    free(scan->mgmtData);  

    return RC_OK;
}

extern int getRecordSize(Schema *schema)
{
	int record_size = 0, attribute_index;
	
	for(attribute_index = 0; attribute_index < schema->numAttr; attribute_index++)
	{
		if(schema->dataTypes[attribute_index] == DT_STRING)
		{
			record_size += schema->typeLength[attribute_index];
		}
		else if(schema->dataTypes[attribute_index] == DT_INT)
		{
			record_size += sizeof(int);
		}
		else if(schema->dataTypes[attribute_index] == DT_FLOAT)
		{
			record_size += sizeof(float);
		}
		else if(schema->dataTypes[attribute_index] == DT_BOOL)
		{
			record_size += sizeof(bool);
		}
	}
	return ++record_size;
}


Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    if (checkNullInputs(numAttr, attrNames, dataTypes, typeLength)) {
        return NULL; 
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL) {
        return NULL; 
    }

    schema->numAttr = numAttr;
    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));
    schema->keyAttrs = (int *)malloc(keySize * sizeof(int));
    schema->keySize = keySize;

    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL || schema->keyAttrs == NULL) {
        free(schema);
        return NULL;
    }

    int i = 0;
    while (i < numAttr) {
        schema->attrNames[i] = strdup(attrNames[i]);
        schema->dataTypes[i] = dataTypes[i];
        schema->typeLength[i] = typeLength[i];
        i++;
    }

    // Copy key attributes if provided
    if (keySize > 0 && keys != NULL) {
        int j = 0;
        while (j < keySize) {
            schema->keyAttrs[j] = keys[j];
            j++;
        }
    }

    return schema; 
}

int checkNullInputs(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength) {
    return (numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL);
}

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema (Schema *schema)
{
	free(schema);
	return RC_OK;
}


extern RC createRecord(Record **new_record, Schema *schema)
{
    Record *freshRecord = (Record*) malloc(sizeof(Record));
	
    int length = getRecordSize(schema);

    freshRecord->data = (char*) malloc(length);

    freshRecord->id.page = freshRecord->id.slot = -1;

    char *char_pointer = freshRecord->data;

    char placeholder = '-';
    *char_pointer = placeholder;

    *(++char_pointer) = '\0';

    *new_record = freshRecord;

    return RC_OK;
}



// Set the offset (in bytes) from the initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset(Schema *schema, int attribute_number, int *offset_result)
{
	int attribute_index;
	*offset_result = 1;

	for(attribute_index = 0; attribute_index < attribute_number; attribute_index++)
	{
		if (schema->dataTypes[attribute_index] == DT_STRING)
		{
			*offset_result = *offset_result + schema->typeLength[attribute_index];
		}
		else if (schema->dataTypes[attribute_index] == DT_INT)
		{
			*offset_result = *offset_result + sizeof(int);
		}
		else if (schema->dataTypes[attribute_index] == DT_FLOAT)
		{
			*offset_result = *offset_result + sizeof(float);
		}
		else if (schema->dataTypes[attribute_index] == DT_BOOL)
		{
			*offset_result = *offset_result + sizeof(bool);
		}
	}
	return RC_OK;
}


// This function removes the record from the memory.
extern RC freeRecord (Record *record)
{
	free(record);
	return RC_OK;
}

// This function retrieves an attribute from the given record in the specified schema
extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    int position = 0;

    attrOffset(schema, attrNum, &position);

    Value *field = (Value*) malloc(sizeof(Value));
    char *data_location = record->data;
    data_location = data_location + position;

    if (attrNum == 1) {
        schema->dataTypes[attrNum] = 1;
    } else {
        schema->dataTypes[attrNum] = schema->dataTypes[attrNum];
    }

    if (schema->dataTypes[attrNum] == DT_STRING) {
        int length = schema->typeLength[attrNum];
        field->v.stringV = (char *) malloc(length + 1);
        strncpy(field->v.stringV, data_location, length);
        field->v.stringV[length] = '\0';
        field->dt = DT_STRING;
    } else if (schema->dataTypes[attrNum] == DT_INT) {
        int value = 0;
        memcpy(&value, data_location, sizeof(int));
        field->v.intV = value;
        field->dt = DT_INT;
    } else if (schema->dataTypes[attrNum] == DT_FLOAT) {
        float value;
        memcpy(&value, data_location, sizeof(float));
        field->v.floatV = value;
        field->dt = DT_FLOAT;
    } else if (schema->dataTypes[attrNum] == DT_BOOL) {
        bool value;
        memcpy(&value, data_location, sizeof(bool));
        field->v.boolV = value;
        field->dt = DT_BOOL;
    } else {
        printf("Error: Serializer not defined for the given datatype. \n");
    }

    *value = field;
    return RC_OK;
}




extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int new_offset = 1;
    int i;

    for (i = 0; i < attrNum; i++) {
        if (schema->dataTypes[i] == DT_STRING) {
            new_offset += schema->typeLength[i];
        } else if (schema->dataTypes[i] == DT_INT) {
            new_offset += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            new_offset += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            new_offset += sizeof(bool);
        } else {
            printf("Serializer not defined for the given datatype. \n");
            return RC_INVALID_DATATYPE;
        }
    }

    char *new_dataPointer = record->data + new_offset;

    if (schema->dataTypes[attrNum] == DT_STRING) {
        int length = schema->typeLength[attrNum];
        if (strlen(value->v.stringV) > length) {
            strncpy(new_dataPointer, value->v.stringV, length);
            new_dataPointer[length] = '\0';
        } else {
            strcpy(new_dataPointer, value->v.stringV);
        }
    } else if (schema->dataTypes[attrNum] == DT_INT) {
        *(int *) new_dataPointer = value->v.intV;
    } else if (schema->dataTypes[attrNum] == DT_FLOAT) {
        *(float *) new_dataPointer = value->v.floatV;
    } else if (schema->dataTypes[attrNum] == DT_BOOL) {
        *(bool *) new_dataPointer = value->v.boolV;
    } else {
        printf("Serializer not defined for the given datatype. \n");
        return RC_INVALID_DATATYPE;
    }

    return RC_OK;
}


#define MAX_COMMAND_LENGTH 100

void printMenu() {
    printf("\nTable Operations:\n");
    printf("1. Insert Record\n");
    printf("2. Update Record\n");
    printf("3. Delete Record\n");
    printf("4. Exit\n");
}

void executeCommand(int command, RM_TableData *rel, Record *record) {
    switch(command) {
        case 1:
            // Call the function to create a table
            createTable("TableName", rel->schema);
            break;
        case 2:
            // Call the function to open a table
            openTable(rel, "TableName");
            break;
        case 3:
            // Call the function to insert a record
            insertRecord(rel, record);
            break;
        case 4:
            // Call the function to delete a record
            deleteRecord(rel, record->id);
            break;
        case 5:
            // Call the function to update a record
            updateRecord(rel, record);
            break;
        case 6:
            // Call the function to get a record
            getRecord(rel, record->id, record);
            break;
        default:
            printf("Invalid command\n");
            break;
    }
}