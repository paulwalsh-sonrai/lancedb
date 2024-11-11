use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, Query as LanceDbQuery, QueryBase, QueryExecutionOptions, Select, VectorQuery as LanceDbVectorQuery};
use tokio::runtime::Runtime;
use std::ffi::CString;

use crate::error::convert_error;
use crate::iterator::RecordBatchIterator;
use crate::util::parse_distance_type;

use tokio::sync::Mutex;
#[repr(C)]
pub struct Query {
    inner: LanceDbQuery,
}

impl Query {
    pub fn new(query: LanceDbQuery) -> Self {
        Self { inner: query }
    }

    pub fn only_if(&mut self, predicate: &str) {
        self.inner = self.inner.clone().only_if(predicate.to_string());
    }

    pub fn full_text_search(&mut self, query: &str, columns: Option<Vec<String>>) {
        let query = FullTextSearchQuery::new(query.to_string()).columns(columns);
        self.inner = self.inner.clone().full_text_search(query);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    pub fn nearest_to(&mut self, vector: &[f32]) -> Result<VectorQuery, String> {
        let inner = self.inner.clone().nearest_to(vector).map_err(|e| e.to_string())?;
        Ok(VectorQuery { inner })
    }

    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    pub async fn execute(&self, max_batch_length: Option<u32>) -> Result<RecordBatchIterator, String> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        let inner_stream = self.inner.execute_with_options(execution_opts)
            .await
            .map_err(|e| format!("Failed to execute query stream: {}", convert_error(&e)))?;
        Ok(RecordBatchIterator::new(inner_stream))
    }

    pub async fn explain_plan(&self, verbose: bool) -> Result<String, String> {
        self.inner.explain_plan(verbose).await.map_err(|e| {
            format!("Failed to retrieve the query plan: {}", convert_error(&e))
        })
    }
}

#[repr(C)]
pub struct VectorQuery {
    inner: LanceDbVectorQuery,
}

impl VectorQuery {
    pub fn column(&mut self, column: &str) {
        self.inner = self.inner.clone().column(column);
    }

    pub fn distance_type(&mut self, distance_type: &str) -> Result<(), String> {
        let distance_type = parse_distance_type(distance_type.to_string())?;
        self.inner = self.inner.clone().distance_type(distance_type);
        Ok(())
    }

    pub fn postfilter(&mut self) {
        self.inner = self.inner.clone().postfilter();
    }

    pub fn refine_factor(&mut self, refine_factor: u32) {
        self.inner = self.inner.clone().refine_factor(refine_factor);
    }

    pub fn nprobes(&mut self, nprobe: u32) {
        self.inner = self.inner.clone().nprobes(nprobe as usize);
    }

    pub fn bypass_vector_index(&mut self) {
        self.inner = self.inner.clone().bypass_vector_index()
    }

    pub fn only_if(&mut self, predicate: &str) {
        self.inner = self.inner.clone().only_if(predicate.to_string());
    }

    pub fn full_text_search(&mut self, query: &str, columns: Option<Vec<String>>) {
        let query = FullTextSearchQuery::new(query.to_string()).columns(columns);
        self.inner = self.inner.clone().full_text_search(query);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    pub async fn execute(&self, max_batch_length: Option<u32>) -> Result<RecordBatchIterator, String> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        let inner_stream = self.inner.execute_with_options(execution_opts)
            .await
            .map_err(|e| format!("Failed to execute query stream: {}", convert_error(&e)))?;
        Ok(RecordBatchIterator::new(inner_stream))
    }

    pub async fn explain_plan(&self, verbose: bool) -> Result<String, String> {
        self.inner.explain_plan(verbose).await.map_err(|e| {
            format!("Failed to retrieve the query plan: {}", convert_error(&e))
        })
    }
}




#[no_mangle]
pub extern "C" fn set_distance_type(query: *mut VectorQuery, distance_type: *const libc::c_char) -> libc::c_int {
    let query = unsafe { &mut *query };
    let distance_type = unsafe { std::ffi::CStr::from_ptr(distance_type).to_str().unwrap_or("euclidean") };
    
    if let Err(_) = query.distance_type(distance_type) {
        return -1;
    }
    0
}

#[no_mangle]
pub extern "C" fn set_limit(query: *mut VectorQuery, limit: u32) {
    let query = unsafe { &mut *query };
    query.limit(limit);
}


#[no_mangle]
pub extern "C" fn execute_query(query: *mut VectorQuery, max_batch_length: u32) -> *mut RecordBatchIterator {
    let query = unsafe { &*query };
// Attempt to create a new Tokio runtime
    let runtime = Runtime::new().unwrap();

    // Execute the query using the runtime
    match runtime.block_on(query.execute(Some(max_batch_length))) {
        Ok(iterator) => Box::into_raw(Box::new(iterator)),
        Err(e) => {
            // Format the error message
            let error_message = format!("Failed to execute query: {}", e);

            // Print the error message to stdout
            println!("{}", error_message);
            std::ptr::null_mut() // Return null pointer to indicate failure
        }
    }
}

