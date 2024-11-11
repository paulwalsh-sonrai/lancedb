// NOTE: You could use https://michael-f-bryan.github.io/rust-ffi-guide/cbindgen.html to generate
// this header automatically from your Rust code.  But for now, we'll just write it by hand.

#include <stdint.h>  // For standard integer types
#include <stdlib.h>  // For NULL definition



// Forward declaration of Connection and Table structures
typedef struct Connection Connection;
typedef struct Table Table;
typedef struct Query Query;
typedef struct VectorQuery VectorQuery;
typedef struct RecordBatchIterator RecordBatchIterator;


// Struct for ConnectionOptions, matching fields in Rust struct
typedef struct {
    double read_consistency_interval;  // Read consistency interval in seconds
    const char* api_key;               // API key
    const char* region;                // AWS region (e.g., "us-east-1")
    const char* host_override;         // Optional host override
    // Add additional fields as needed
} ConnectionOptions;

/**
 * Creates a new LanceDB connection.
 * 
 * @param uri The URI as a null-terminated C string.
 * @param options The connection options.
 * @return Pointer to the Connection struct, or NULL on failure.
 */
Connection* create_connection(const char* uri);

/**
 * Frees the LanceDB connection.
 * 
 * @param conn Pointer to the Connection struct to free.
 */
void free_connection(Connection* conn);

/**
 * Creates a new table in the LanceDB connection.
 * 
 * @param conn Pointer to the existing Connection struct.
 * @param name The name of the table as a null-terminated C string.
 * @param buf Pointer to the buffer holding IPC file data.
 * @param buf_len Length of the buffer.
 * @param mode The mode for creating the table (e.g., "create", "overwrite").
 * @return Pointer to the Table struct, or NULL on failure.
 */
Table* create_table_c(const Connection* conn, const char* name, const uint8_t* buf, size_t buf_len, const char* mode);

/**
 * Opens an existing table in the LanceDB connection.
 * 
 * @param conn Pointer to the existing Connection struct.
 * @param name The name of the table as a null-terminated C string.
 * @return Pointer to the Table struct, or NULL on failure.
 */
Table* open_table_c(const Connection* conn, const char* name);

/**
 * Frees the LanceDB table.
 * 
 * @param table Pointer to the Table struct to free.
 */
void free_table(Table* table);

/**
 * Adds data to the table.
 * 
 * @param table Pointer to the Table struct.
 * @param buf Pointer to the buffer holding IPC file data.
 * @param buf_len Length of the buffer.
 * @param mode The mode for adding data (e.g., "append", "overwrite").
 * @return Null-terminated C string "Success" on success, or an error message on failure.
 *         The caller is responsible for freeing the string with free_cstring.
 */
char* table_add(const Table* table, const uint8_t* buf, size_t buf_len, const char* mode);

/**
 * Retrieves the schema of the table as an IPC file.
 * 
 * @param table Pointer to the Table struct.
 * @return Pointer to a null-terminated C string containing the schema IPC data,
 *         or NULL on error. The caller is responsible for freeing the string with free_cstring.
 */
char* table_schema(const Table* table);


/**
 * Gets a string representation of the table.
 * 
 * @param table Pointer to the Table struct.
 * @return Null-terminated C string representing the table, or NULL on error. 
 *         The caller is responsible for freeing the string with free_cstring.
 */
char* table_display(const Table* table);



Query* table_query(const Table* table);


/**
 * Counts the rows in the table.
 * 
 * @param table_ptr Pointer to the CTable struct.
 * @param filter Optional filter as a null-terminated C string.
 *               Pass NULL if no filter is required.
 * @return The number of rows in the table, or -1 on error.
 */
int64_t table_count_rows(const Table* table_ptr, const char* filter);

/**
 * Performs a vector search on the table.
 * 
 * @param table_ptr Pointer to the CTable struct.
 * @param vector_ptr Pointer to an array of float values representing the search vector.
 * @param vector_len Length of the vector.
 * @return Pointer to the VectorQuery struct, or NULL on error.
 */
VectorQuery* table_vector_search(Table* table_ptr, const float* vector_ptr, size_t vector_len);



/**
 * Sets the limit on the number of results for the VectorQuery.
 *
 * @param query Pointer to the VectorQuery object.
 * @param limit The maximum number of results to return.
 */
void set_limit(VectorQuery* query, uint32_t limit);

/**
 * Executes the query and returns an iterator for the result set.
 *
 * @param query Pointer to the VectorQuery object.
 * @param max_batch_length The maximum number of records per batch.
 * @return Pointer to the RecordBatchIterator, or NULL on error.
 */
RecordBatchIterator* execute_query(VectorQuery* query, uint32_t max_batch_length);
