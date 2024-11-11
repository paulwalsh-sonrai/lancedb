use arrow_ipc::writer::FileWriter;
use lancedb::ipc::ipc_file_to_batches;
use lancedb::table::{
    AddDataMode, Table as LanceDbTable
};
use std::ffi::{CString, CStr};
use std::os::raw::{c_char};
use std::ptr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use crate::query::{Query, VectorQuery};

pub struct Table {
    // We keep a duplicate of the table name so we can use it for error
    // messages even if the table has been closed
    pub name: String,
    pub(crate) inner: Option<LanceDbTable>,
}

impl Table {
    fn inner_ref(&self) -> Result<&LanceDbTable, String> {
        self.inner
            .as_ref()
            .ok_or_else(|| format!("Table {} is closed", self.name))
    }
}

impl Table {
    pub(crate) fn new(table: LanceDbTable) -> Self {
        Self {
            name: table.name().to_string(),
            inner: Some(table),
        }
    }

    pub fn display(&self) -> String {
        match &self.inner {
            None => format!("ClosedTable({})", self.name),
            Some(inner) => inner.to_string(),
        }
    }

    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    pub fn close(&mut self) {
        self.inner.take();
    }

    pub async fn schema(&self) -> Result<Vec<u8>, String> {
        let schema = self.inner_ref()?.schema().await.map_err(|e| e.to_string())?;
        let mut writer = FileWriter::try_new(vec![], &schema)
            .map_err(|e| format!("Failed to create IPC file: {}", e))?;
        writer
            .finish()
            .map_err(|e| format!("Failed to finish IPC file: {}", e))?;
        writer.into_inner().map_err(|e| format!("Failed to get IPC file: {}", e))
    }

    pub async fn add(&self, buf: Vec<u8>, mode: String) -> Result<(), String> {
        let batches = ipc_file_to_batches(buf)
            .map_err(|e| format!("Failed to read IPC file: {}", e))?;
        let mut op = self.inner_ref()?.add(batches);

        op = match mode.as_str() {
            "append" => op.mode(AddDataMode::Append),
            "overwrite" => op.mode(AddDataMode::Overwrite),
            _ => return Err(format!("Invalid mode: {}", mode)),
        };

        op.execute().await.map_err(|e| e.to_string())
    }

    pub async fn count_rows(&self, filter: Option<String>) -> Result<i64, String> {
        self.inner_ref()?
            .count_rows(filter)
            .await
            .map(|val| val as i64)
            .map_err(|e| e.to_string())
    }

    pub async fn delete(&self, predicate: String) -> Result<(), String> {
        self.inner_ref()?.delete(&predicate).await.map_err(|e| e.to_string())
    }

    pub fn query(&self) -> Result<Query, String> {
        Ok(Query::new(self.inner_ref()?.query()))
    }

    pub fn vector_search(&self, vector: Vec<f32>) -> Result<VectorQuery, String> {
        self.query()?.nearest_to(&vector)
    }

    
}




// C-compatible Table struct pointer type
#[repr(C)]
pub struct CTable {
    pub(crate) inner: Arc<Table>,
}

// Wrappers for async execution in Rust
fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
    let rt = Runtime::new().unwrap();
    rt.block_on(future)
}


#[no_mangle]
pub extern "C" fn table_display(table_ptr: *mut CTable) -> *mut c_char {
    let table = unsafe {
        assert!(!table_ptr.is_null());
        &*(*table_ptr).inner
    };
    let display_string = table.display();
    CString::new(display_string).unwrap().into_raw()
}

#[no_mangle]
pub extern "C" fn table_schema(table_ptr: *mut CTable) -> *mut c_char {
    let table = unsafe {
        assert!(!table_ptr.is_null());
        &*(*table_ptr).inner
    };
    match run_async(table.schema()) {
        Ok(schema) => CString::new(schema).unwrap().into_raw(),
        Err(e) => CString::new(format!("Error: {}", e)).unwrap().into_raw(),
    }
}



#[no_mangle]
pub extern "C" fn table_query(table_ptr: *mut CTable) -> *mut Query {
    let table = unsafe {
        assert!(!table_ptr.is_null());
        &*(*table_ptr).inner
    };
    match table.query() {
        Ok(query) => Box::into_raw(Box::new(query)),
        Err(_) => ptr::null_mut(), // Return null on error
    }
}

// Cleanup function for strings allocated with CString
#[no_mangle]
pub extern "C" fn free_cstring(s: *mut c_char) {
    if s.is_null() { return; }
    unsafe {
        CString::from_raw(s); // Reclaims memory
    }
}

// Cleanup function for Table
#[no_mangle]
pub extern "C" fn free_table(table_ptr: *mut CTable) {
    if table_ptr.is_null() { return; }
    unsafe {
        Box::from_raw(table_ptr);
    }
}


#[no_mangle]
pub extern "C" fn table_count_rows(table_ptr: *mut CTable, filter: *const c_char) -> i64 {
    let table = unsafe {
        assert!(!table_ptr.is_null());
        &*(*table_ptr).inner
    };

    // Convert the filter from C string to Rust String, if provided
    let filter = if filter.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(filter).to_string_lossy().into_owned() })
    };

    // Run the async count_rows function synchronously and handle errors
    match run_async(table.count_rows(filter)) {
        Ok(count) => count,
        Err(e) => {
            eprintln!("Error in count_rows: {}", e);
            -1 // Return -1 to indicate an error
        }
    }
}

#[no_mangle]
pub extern "C" fn table_vector_search(table_ptr: *mut CTable, vector_ptr: *const f32, vector_len: usize) -> *mut VectorQuery {
    let table = unsafe {
        assert!(!table_ptr.is_null());
        &*(*table_ptr).inner
    };

    // Convert the raw C array to a Vec<f32>
    let vector = unsafe {
        assert!(!vector_ptr.is_null());
        std::slice::from_raw_parts(vector_ptr, vector_len).to_vec()
    };

    // Call vector_search and handle the result
    match table.vector_search(vector) {
        Ok(query) => Box::into_raw(Box::new(query)),
        Err(e) => {
            eprintln!("Error in vector_search: {}", e);
            ptr::null_mut() // Return NULL to indicate an error
        }
    }
}
