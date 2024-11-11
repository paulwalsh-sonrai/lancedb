
use std::collections::HashMap;
use std::str::FromStr;
use std::ffi::{CStr};

use std::os::raw::c_char;
use std::sync::Arc;
use lancedb::ipc::ipc_file_to_batches;

use tokio::runtime::Runtime;
use lancedb::connection::{Connection as LanceDBConnection, CreateTableMode, LanceFileVersion, ConnectBuilder};

use crate::error::convert_error;
use crate::table::{Table, CTable};


#[repr(C)]
pub struct Connection {
    inner: Option<LanceDBConnection>,
}

impl Connection {
    pub(crate) fn inner_new(inner: LanceDBConnection) -> Self {
        Self { inner: Some(inner) }
    }

    fn get_inner(&self) -> Result<&LanceDBConnection, String> {
        self.inner.as_ref().ok_or_else(|| "Connection is closed".to_string())
    }
    
    fn parse_create_mode_str(mode: &str) -> Result<CreateTableMode, String> {
        match mode {
            "create" => Ok(CreateTableMode::Create),
            "overwrite" => Ok(CreateTableMode::Overwrite),
            "exist_ok" => Ok(CreateTableMode::exist_ok(|builder| builder)),
            _ => Err(format!("Invalid mode {}", mode)),
        }
    }
    /// Synchronous version of the new function to be compatible with cgo
    #[no_mangle]
    pub extern "C" fn create_connection(uri: *const c_char) -> *mut Connection {
        // Convert C string to Rust string
        let uri = unsafe { CStr::from_ptr(uri).to_string_lossy().into_owned() };
        // Initialize a synchronous runtime
        let runtime = Runtime::new().unwrap();

        // Execute the asynchronous code in a blocking manner
        let connection = runtime.block_on(async {
            let mut builder = ConnectBuilder::new(&uri);
         

        

            builder = builder.region("us-east-1");
    
            builder.execute().await.map_err(|e| format!("Error executing builder: {}", e))
        });

        match connection {
            Ok(conn) => Box::into_raw(Box::new(Self::inner_new(conn))),
            Err(err) => {
                eprintln!("Failed to create connection: {}", err);
                std::ptr::null_mut() // Return null pointer on failure
            }
        }
    }
        pub async fn create_table(
        &self,
        name: String,
        buf: Vec<u8>,
        mode: String,
        storage_options: Option<HashMap<String, String>>,
        data_storage_options: Option<String>,
        enable_v2_manifest_paths: Option<bool>,
    ) -> Result<Table, String> {
        let batches = ipc_file_to_batches(buf)
            .map_err(|e| format!("Failed to read IPC file: {}", e))?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let mut builder = self.get_inner()?.create_table(&name, batches).mode(mode);

        if let Some(storage_options) = storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        if let Some(data_storage_option) = data_storage_options.as_ref() {
            builder = builder.data_storage_version(
                LanceFileVersion::from_str(data_storage_option).map_err(|e| convert_error(&e))?,
            );
        }
        if let Some(enable_v2_manifest_paths) = enable_v2_manifest_paths {
            builder = builder.enable_v2_manifest_paths(enable_v2_manifest_paths);
        }
        // Await the execution of the future and handle the result.
        let tbl = builder.execute().await.map_err(|e| format!("Error executing builder: {:?}", e))?;  // Convert `lancedb::Error` to `String`

        Ok(Table::new(tbl))
    }

    pub async fn open_table(
        &self,
        name: String,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
    ) -> Result<Table, String> {
        let mut builder = self.get_inner()?.open_table(&name);
        if let Some(storage_options) = storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        if let Some(index_cache_size) = index_cache_size {
            builder = builder.index_cache_size(index_cache_size);
        }
        
        // Await the execution of the future and handle the result.
        let tbl = builder.execute().await.map_err(|e| format!("Error executing builder: {:?}", e))?;  // Convert `lancedb::Error` to `String`

        Ok(Table::new(tbl))
    }
}


#[no_mangle]
pub extern "C" fn create_table_c(
    conn: *const Connection,
    name: *const c_char,
    buf: *const u8,
    buf_len: usize,
    mode: *const c_char,
) -> *mut CTable {
    let runtime = Runtime::new().unwrap();
    let conn = unsafe { &*conn };

    // Convert C strings to Rust strings
    let name = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };
    let mode = unsafe { CStr::from_ptr(mode).to_string_lossy().into_owned() };
    let buffer = unsafe { std::slice::from_raw_parts(buf, buf_len).to_vec() };

    // Call async Rust function in synchronous context
    let result = runtime.block_on(conn.create_table(name, buffer, mode, None, None, None));

    // Handle result
    match result {
        Ok(table) => {
            let c_table = CTable {
                inner: Arc::new(Table::new(table.inner.expect("have fruit"))),
            };
            Box::into_raw(Box::new(c_table))
        }
        Err(err) => {
            eprintln!("Error creating table: {}", err);
            std::ptr::null_mut()
        }
    }
}

/// Wrapper function for open_table for FFI
#[no_mangle]
pub extern "C" fn open_table_c(
    conn: *const Connection,
    name: *const c_char,
    // other parameters as needed
) -> *mut Table {
    let runtime = Runtime::new().unwrap();
    let conn = unsafe { &*conn };

    // Convert C strings to Rust strings
    let name = unsafe { CStr::from_ptr(name).to_string_lossy().into_owned() };

    // Call async Rust function in synchronous context
    let result = runtime.block_on(conn.open_table(name, None, None));

    // Handle result
    match result {
        Ok(table) => Box::into_raw(Box::new(table)),
        Err(err) => {
            eprintln!("Error opening table: {}", err);
            std::ptr::null_mut()
        }
    }
}


#[no_mangle]
pub extern "C" fn free_connection(conn: *mut Connection) {
    if !conn.is_null() {
        unsafe { Box::from_raw(conn) }; // Free the memory
    }
}