use lancedb::{arrow::IntoArrow, ipc::ipc_file_to_batches, table::merge::MergeInsertBuilder};

use crate::error::convert_error;

#[derive(Clone)]
/// A builder used to create and run a merge insert operation
pub struct NativeMergeInsertBuilder {
    pub(crate) inner: MergeInsertBuilder,
}

impl NativeMergeInsertBuilder {
    pub fn when_matched_update_all(&self, condition: Option<String>) -> Self {
        let mut this = self.clone();
        this.inner.when_matched_update_all(condition);
        this
    }

    pub fn when_not_matched_insert_all(&self) -> Self {
        let mut this = self.clone();
        this.inner.when_not_matched_insert_all();
        this
    }

    pub fn when_not_matched_by_source_delete(&self, filter: Option<String>) -> Self {
        let mut this = self.clone();
        this.inner.when_not_matched_by_source_delete(filter);
        this
    }

    pub async fn execute(&self, buf: &[u8]) -> Result<(), String> {
        let data = ipc_file_to_batches(buf.to_vec())
            .and_then(IntoArrow::into_arrow)
            .map_err(|e| format!("Failed to read IPC file: {}", convert_error(&e)))?;

        let this = self.clone();

        this.inner.execute(data).await.map_err(|e| {
            format!("Failed to execute merge insert: {}", convert_error(&e))
        })
    }
}

impl From<MergeInsertBuilder> for NativeMergeInsertBuilder {
    fn from(inner: MergeInsertBuilder) -> Self {
        Self { inner }
    }
}