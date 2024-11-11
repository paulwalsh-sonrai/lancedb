// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::StreamExt;
use lancedb::arrow::SendableRecordBatchStream;
use lancedb::ipc::batches_to_ipc_file;
use std::error::Error;

/// Iterator over RecordBatches
pub struct RecordBatchIterator {
    inner: SendableRecordBatchStream,
}

impl RecordBatchIterator {
    pub(crate) fn new(inner: SendableRecordBatchStream) -> Self {
        Self { inner }
    }

    pub async unsafe fn next(&mut self) -> Result<Option<Vec<u8>>, String> {
        if let Some(rst) = self.inner.next().await {
            let batch = rst.map_err(|e| format!("Failed to get next batch from stream: {}", e))?;
            batches_to_ipc_file(&[batch])
                .map_err(|e| format!("Failed to write IPC file: {}", e))
                .map(|buf| Some(buf))
        } else {
            // We are done with the stream.
            Ok(None)
        }
    }
}
