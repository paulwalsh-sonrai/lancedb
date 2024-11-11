use std::sync::{Mutex};
use lancedb::index::scalar::{BTreeIndexBuilder, FtsIndexBuilder};
use lancedb::index::vector::{IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder};
use lancedb::index::Index as LanceDbIndex;

use crate::util::parse_distance_type;

pub struct Index {
    inner: Mutex<Option<LanceDbIndex>>,
}

impl Index {
    pub fn consume(&self) -> Result<LanceDbIndex, String> {
        self.inner
            .lock()
            .map_err(|_| "Failed to lock the mutex".to_string())?
            .take()
            .ok_or_else(|| "Attempt to use an index more than once".to_string())
    }
}

impl Index {
    pub fn ivf_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
    ) -> Result<Self, String> {
        let mut ivf_pq_builder = IvfPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            ivf_pq_builder = ivf_pq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            ivf_pq_builder = ivf_pq_builder.num_partitions(num_partitions);
        }
        if let Some(num_sub_vectors) = num_sub_vectors {
            ivf_pq_builder = ivf_pq_builder.num_sub_vectors(num_sub_vectors);
        }
        if let Some(max_iterations) = max_iterations {
            ivf_pq_builder = ivf_pq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            ivf_pq_builder = ivf_pq_builder.sample_rate(sample_rate);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfPq(ivf_pq_builder))),
        })
    }

    pub fn btree() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::BTree(BTreeIndexBuilder::default()))),
        }
    }

    pub fn bitmap() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::Bitmap(Default::default()))),
        }
    }

    pub fn label_list() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::LabelList(Default::default()))),
        }
    }

    pub fn fts(with_position: Option<bool>) -> Self {
        let mut opts = FtsIndexBuilder::default();
        if let Some(with_position) = with_position {
            opts = opts.with_position(with_position);
        }
        Self {
            inner: Mutex::new(Some(LanceDbIndex::FTS(opts))),
        }
    }

    pub fn hnsw_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
    ) -> Result<Self, String> {
        let mut hnsw_pq_builder = IvfHnswPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            hnsw_pq_builder = hnsw_pq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            hnsw_pq_builder = hnsw_pq_builder.num_partitions(num_partitions);
        }
        if let Some(num_sub_vectors) = num_sub_vectors {
            hnsw_pq_builder = hnsw_pq_builder.num_sub_vectors(num_sub_vectors);
        }
        if let Some(max_iterations) = max_iterations {
            hnsw_pq_builder = hnsw_pq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            hnsw_pq_builder = hnsw_pq_builder.sample_rate(sample_rate);
        }
        if let Some(m) = m {
            hnsw_pq_builder = hnsw_pq_builder.num_edges(m);
        }
        if let Some(ef_construction) = ef_construction {
            hnsw_pq_builder = hnsw_pq_builder.ef_construction(ef_construction);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfHnswPq(hnsw_pq_builder))),
        })
    }

    pub fn hnsw_sq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
    ) -> Result<Self, String> {
        let mut hnsw_sq_builder = IvfHnswSqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
            hnsw_sq_builder = hnsw_sq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            hnsw_sq_builder = hnsw_sq_builder.num_partitions(num_partitions);
        }
        if let Some(max_iterations) = max_iterations {
            hnsw_sq_builder = hnsw_sq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            hnsw_sq_builder = hnsw_sq_builder.sample_rate(sample_rate);
        }
        if let Some(m) = m {
            hnsw_sq_builder = hnsw_sq_builder.num_edges(m);
        }
        if let Some(ef_construction) = ef_construction {
            hnsw_sq_builder = hnsw_sq_builder.ef_construction(ef_construction);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfHnswSq(hnsw_sq_builder))),
        })
    }
}
