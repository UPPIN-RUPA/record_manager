# Record Manager

## Contribution
1. **Table, Attribute, User Interface for table CRUD and Record Manager Functions:** Manju
2. **Record Functions:** Rupa
3. **Scan Functions:** Vanaja
4. **Schema Functions:** Mohit

## Running the Script

1. Navigate to the project root directory (assign3) using the terminal.
2. Use the `ls` command to list the files and ensure you are in the correct directory.
3. Run `make clean` to delete old compiled `.o` files.
4. Run `make` to compile all project files, including `test_assign3_1.c`.
5. Execute `make run` to run the `test_assign3_1.c` file.
6. To compile test expression-related files, use `make test_expr`.
7. Finally, execute `make run_expr` to run the `test_expr.c` file.

## Solution Description

The Record Manager is responsible for handling records and tables efficiently. It provides functions to manage tables, records, scans, and schemas. Below is a breakdown of each module's functionalities:

### 1. Table and Record Manager Functions

These functions handle the initialization, creation, deletion, opening, and closing of tables. They also manage records within the tables, including insertion, deletion, and updating.

- **`initRecordManager(...)`**: Initializes the record manager.
- **`shutdownRecordManager(...)`**: Shuts down the record manager and deallocates all resources.
- **`createTable(...)`**: Creates a new table with specified attributes.
- **`openTable(...)`**: Opens an existing table.
- **`closeTable(...)`**: Closes a table.
- **`deleteTable(...)`**: Deletes a table.
- **`getNumTuples(...)`**: Returns the number of tuples in a table.

### 2. Record Functions

These functions handle individual records within a table, including insertion, deletion, updating, and retrieval.

- **`insertRecord(...)`**: Inserts a new record into the table.
- **`deleteRecord(...)`**: Deletes a record from the table.
- **`updateRecord(...)`**: Updates an existing record in the table.
- **`getRecord(...)`**: Retrieves a record from the table.

### 3. Scan Functions

These functions manage scans on tables, allowing users to iterate through tuples based on certain conditions.

- **`startScan(...)`**: Initiates a new scan operation on the table.
- **`next(...)`**: Retrieves the next tuple that satisfies the scan condition.
- **`closeScan(...)`**: Closes the current scan operation.

### 4. Schema Functions

These functions deal with schema-related operations, such as creating and managing schemas.

- **`getRecordSize(...)`**: Returns the size of a record in a specified schema.
- **`freeSchema(...)`**: Deallocates memory occupied by a schema.
- **`createSchema(...)`**: Creates a new schema with specified attributes.

### 5. Attribute Functions

These functions handle attribute-related operations within records, including attribute retrieval, setting, and offset calculation.

- **`attrOffset(...)`**: Calculates the offset of a specified attribute within a record.
- **`freeRecord(...)`**: Deallocates memory occupied by a record.
- **`getAttr(...)`**: Retrieves an attribute from a record.
- **`setAttr(...)`**: Sets the value of an attribute in a record.
