// Copyright (c) 2025 ADBC Drivers Contributors
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

//! Result readers for fetching query results from Databricks.
//!
//! This module provides:
//! - `ResultReaderFactory`: Creates appropriate reader based on response type
//! - `CloudFetchReader`: Streams Arrow data via cloud storage downloads
//! - `InlineArrowProvider`: Handles inline Arrow data embedded in responses

pub mod cloudfetch;
pub mod inline;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::client::{DatabricksClient, DatabricksHttpClient, ExecuteResponse};
use crate::error::{DatabricksErrorHelper, Result};
use crate::reader::cloudfetch::chunk_downloader::ChunkDownloader;
use crate::reader::cloudfetch::link_fetcher::{SeaChunkLinkFetcher, SeaChunkLinkFetcherHandle};
use crate::reader::cloudfetch::streaming_provider::StreamingCloudFetchProvider;
use crate::reader::inline::InlineArrowProvider;
use crate::types::cloudfetch::{CloudFetchConfig, CloudFetchLink};
use crate::types::sea::{CompressionCodec, ResultManifest, StatementState};
use arrow_array::RecordBatch;
use arrow_array::{Array, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use driverbase::error::ErrorHelper;
use std::collections::HashMap;
use std::sync::Arc;

pub use cloudfetch::StreamingCloudFetchProvider as CloudFetchReader;

/// Factory that creates the appropriate reader based on the response type.
///
/// Encapsulates the decision of CloudFetch vs inline vs other result formats.
/// Uses `DatabricksClient` trait for backend flexibility - works with SeaClient
/// or any future backend implementation.
#[derive(Debug)]
pub struct ResultReaderFactory {
    client: Arc<dyn DatabricksClient>,
    http_client: Arc<DatabricksHttpClient>,
    config: CloudFetchConfig,
    runtime_handle: tokio::runtime::Handle,
}

impl ResultReaderFactory {
    /// Create a new result reader factory.
    pub fn new(
        client: Arc<dyn DatabricksClient>,
        http_client: Arc<DatabricksHttpClient>,
        config: CloudFetchConfig,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            client,
            http_client,
            config,
            runtime_handle,
        }
    }

    /// Create a reader from a statement execution response.
    ///
    /// Automatically selects CloudFetch, inline, or other reader based on response.
    pub(crate) fn create_reader(
        &self,
        statement_id: &str,
        response: &ExecuteResponse,
    ) -> Result<Box<dyn ResultReader + Send>> {
        // Log manifest info for debugging
        if let Some(ref manifest) = response.manifest {
            tracing::debug!(
                "Result manifest: total_chunks={:?}, total_rows={:?}, total_bytes={:?}, truncated={}",
                manifest.total_chunk_count,
                manifest.total_row_count,
                manifest.total_byte_count,
                manifest.truncated
            );
        }

        // Determine compression from manifest
        let compression = response
            .manifest
            .as_ref()
            .and_then(|m| m.result_compression.as_deref())
            .map(|s| CompressionCodec::from_manifest(Some(s)))
            .unwrap_or_default();

        // Check result data for CloudFetch or inline results
        if let Some(ref result) = response.result {
            // Log what result data we have for debugging the decision path
            tracing::debug!(
                "Result data present: external_links={}, inline_arrow_data={} bytes, next_chunk_internal_link={}",
                result.external_links.as_ref().map(|l| l.len()).unwrap_or(0),
                result.inline_arrow_data.as_ref().map(|d| d.len()).unwrap_or(0),
                result.next_chunk_internal_link.is_some()
            );

            // Priority 1: CloudFetch (external_links present)
            if let Some(ref external_links) = result.external_links {
                if !external_links.is_empty() {
                    let total_chunk_count =
                        response.manifest.as_ref().and_then(|m| m.total_chunk_count);

                    tracing::info!(
                        "Using CloudFetch reader: {} external links, total_chunks={:?}",
                        external_links.len(),
                        total_chunk_count
                    );

                    return self.create_cloudfetch_reader(
                        statement_id,
                        external_links,
                        total_chunk_count,
                        compression,
                    );
                }
            }

            // Priority 2: Inline Arrow (attachment present)
            if let Some(ref inline_data) = result.inline_arrow_data {
                if !inline_data.is_empty() {
                    tracing::info!(
                        "Using inline Arrow reader: {} bytes of Arrow IPC data",
                        inline_data.len()
                    );

                    return self.create_inline_reader(inline_data, compression);
                }
            }

            // Internal links without external = CloudFetch not available
            if result.next_chunk_internal_link.is_some() {
                return Err(DatabricksErrorHelper::not_implemented()
                    .message("CloudFetch not available. Internal links not supported."));
            }
        }

        // No result data - check if this is a valid empty result or an error state
        match response.status.state {
            StatementState::Succeeded | StatementState::Closed => {
                // Valid empty result set - extract schema from manifest
                // Note: Closed state is valid for inline results where the server delivers
                // the data (or empty result) and immediately closes the statement.
                tracing::info!(
                    "Using empty reader: no result data present for {:?} statement",
                    response.status.state
                );
                let schema = self.extract_schema_from_manifest(response.manifest.as_ref())?;
                Ok(Box::new(EmptyReader::new(schema)))
            }
            StatementState::Pending | StatementState::Running => {
                Err(DatabricksErrorHelper::invalid_state()
                    .message("Statement is still executing. Poll for completion first."))
            }
            StatementState::Failed => {
                let error_msg = response
                    .status
                    .error
                    .as_ref()
                    .and_then(|e| e.message.as_deref())
                    .unwrap_or("Unknown error");
                Err(DatabricksErrorHelper::io().message(format!("Statement failed: {}", error_msg)))
            }
            StatementState::Canceled => {
                Err(DatabricksErrorHelper::io().message("Statement was canceled"))
            }
        }
    }

    /// Extract Arrow schema from the result manifest.
    ///
    /// Falls back to an empty schema if no manifest is available.
    fn extract_schema_from_manifest(&self, manifest: Option<&ResultManifest>) -> Result<SchemaRef> {
        let Some(manifest) = manifest else {
            tracing::warn!("No manifest available for empty result set, using empty schema");
            return Ok(Arc::new(Schema::empty()));
        };

        let fields: Vec<Field> = manifest
            .schema
            .columns
            .iter()
            .map(|col| {
                let data_type = Self::map_databricks_type(&col.type_name);
                Field::new(&col.name, data_type, true) // nullable by default
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Map Databricks SQL type names to Arrow DataTypes.
    fn map_databricks_type(type_name: &str) -> DataType {
        // Databricks type names from the API
        match type_name.to_uppercase().as_str() {
            "BOOLEAN" => DataType::Boolean,
            "BYTE" | "TINYINT" => DataType::Int8,
            "SHORT" | "SMALLINT" => DataType::Int16,
            "INT" | "INTEGER" => DataType::Int32,
            "LONG" | "BIGINT" => DataType::Int64,
            "FLOAT" | "REAL" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "STRING" => DataType::Utf8,
            "BINARY" => DataType::Binary,
            "DATE" => DataType::Date32,
            "TIMESTAMP" | "TIMESTAMP_NTZ" => {
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
            }
            // For complex types and unknown types, fall back to string
            // The actual Arrow IPC data will have the correct type
            _ => {
                tracing::debug!("Unknown Databricks type '{}', mapping to Utf8", type_name);
                DataType::Utf8
            }
        }
    }

    fn create_cloudfetch_reader(
        &self,
        statement_id: &str,
        external_links: &[CloudFetchLink],
        total_chunk_count: Option<i64>,
        compression: CompressionCodec,
    ) -> Result<Box<dyn ResultReader + Send>> {
        tracing::debug!(
            "Creating CloudFetch reader: {} initial links, total_chunks={:?}, prefetch_window={}",
            external_links.len(),
            total_chunk_count,
            self.config.link_prefetch_window
        );

        // Create link fetcher with caching and prefetching built-in
        let fetcher = SeaChunkLinkFetcher::new(
            self.client.clone(),
            statement_id.to_string(),
            external_links,
            total_chunk_count,
            self.config.link_prefetch_window as i64,
            self.runtime_handle.clone(),
        );
        let link_fetcher: Arc<dyn crate::reader::cloudfetch::ChunkLinkFetcher> =
            Arc::new(SeaChunkLinkFetcherHandle::new(fetcher));

        let chunk_downloader = Arc::new(ChunkDownloader::new(
            self.http_client.clone(),
            compression,
            self.config.speed_threshold_mbps,
        ));

        let provider = StreamingCloudFetchProvider::new(
            self.config.clone(),
            link_fetcher,
            chunk_downloader,
            self.runtime_handle.clone(),
        );

        Ok(Box::new(CloudFetchResultReader::new(
            provider,
            self.runtime_handle.clone(),
        )))
    }

    fn create_inline_reader(
        &self,
        inline_data: &[u8],
        compression: CompressionCodec,
    ) -> Result<Box<dyn ResultReader + Send>> {
        tracing::debug!(
            "Creating inline Arrow reader: {} bytes, compression={:?}",
            inline_data.len(),
            compression
        );

        let provider = InlineArrowProvider::new(inline_data.to_vec(), compression)?;

        Ok(Box::new(InlineArrowReader::new(provider)))
    }
}

/// Trait for result readers.
pub trait ResultReader: Send {
    /// Get the schema of the result.
    fn schema(&self) -> Result<SchemaRef>;

    /// Get the next record batch, or None if end of results.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Wrapper for StreamingCloudFetchProvider that implements ResultReader.
struct CloudFetchResultReader {
    provider: Arc<StreamingCloudFetchProvider>,
    runtime_handle: tokio::runtime::Handle,
}

impl CloudFetchResultReader {
    fn new(
        provider: Arc<StreamingCloudFetchProvider>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            provider,
            runtime_handle,
        }
    }
}

impl ResultReader for CloudFetchResultReader {
    fn schema(&self) -> Result<SchemaRef> {
        self.runtime_handle.block_on(self.provider.get_schema())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.runtime_handle.block_on(self.provider.next_batch())
    }
}

/// Reader for inline Arrow results.
///
/// Wraps `InlineArrowProvider` to implement the `ResultReader` trait.
struct InlineArrowReader {
    provider: InlineArrowProvider,
    /// Cached schema (extracted from first batch on creation)
    schema: Option<SchemaRef>,
}

impl InlineArrowReader {
    fn new(provider: InlineArrowProvider) -> Self {
        let schema = provider.schema().cloned();
        Self { provider, schema }
    }
}

impl ResultReader for InlineArrowReader {
    fn schema(&self) -> Result<SchemaRef> {
        self.schema.clone().ok_or_else(|| {
            DatabricksErrorHelper::invalid_state().message("Schema not available for inline result")
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.provider.next_batch()
    }
}

/// Empty reader for queries with no results.
///
/// Used for valid queries that return zero rows (e.g., `SELECT * WHERE 1=0`).
/// The schema is preserved from the query's manifest.
pub struct EmptyReader {
    schema: SchemaRef,
}

impl EmptyReader {
    /// Create an empty reader that returns no batches.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl ResultReader for EmptyReader {
    fn schema(&self) -> Result<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

/// Arrow field metadata keys for Databricks column information.
///
/// These propagate SEA manifest metadata through the Arrow C Data Interface
/// so downstream consumers (ODBC driver) can access server-provided metadata
/// without reverse-engineering from Arrow type IDs.
///
/// Keys use the `databricks.` prefix to avoid conflicts with other metadata
/// conventions (e.g., Arrow's own `ARROW:extension:name`).
pub mod metadata_keys {
    pub const TYPE_NAME: &str = "databricks.type_name";
    pub const TYPE_TEXT: &str = "databricks.type_text";
    pub const TYPE_PRECISION: &str = "databricks.type_precision";
    pub const TYPE_SCALE: &str = "databricks.type_scale";
    pub const TYPE_INTERVAL_TYPE: &str = "databricks.type_interval_type";
}

/// Adapter to make ResultReader work as arrow's RecordBatchReader.
///
/// When `use_geoarrow` is enabled, detects Databricks geometry columns
/// (`Struct<srid: Int32, wkb: Binary>`) and converts them to standard
/// GeoArrow `geoarrow.wkb` extension type (Binary with Arrow extension
/// metadata). This enables zero-copy geometry passthrough to DuckDB,
/// pandas/geopandas, polars, and other GeoArrow-aware consumers.
pub struct ResultReaderAdapter {
    inner: Box<dyn ResultReader + Send>,
    schema: SchemaRef,
    /// Column indices that are Databricks geometry structs to be converted.
    geo_columns: Vec<usize>,
    /// Whether CRS metadata has been captured from the first batch's SRIDs.
    schema_finalized: bool,
}

impl ResultReaderAdapter {
    /// Create a new adapter wrapping a ResultReader, optionally augmenting the
    /// schema with Databricks column metadata from the SEA manifest and
    /// converting geometry structs to GeoArrow.
    pub fn new(
        inner: Box<dyn ResultReader + Send>,
        manifest: Option<&ResultManifest>,
        use_geoarrow: bool,
    ) -> Result<Self> {
        let schema = inner.schema()?;
        let schema = match manifest {
            Some(m) => Self::augment_schema_with_manifest(&schema, m),
            None => schema,
        };

        // Detect geometry columns if geoarrow conversion is enabled
        let geo_columns = if use_geoarrow {
            Self::detect_geometry_columns(&schema)
        } else {
            vec![]
        };

        // Rewrite schema: replace geometry Struct fields with Binary + geoarrow metadata
        let schema = if !geo_columns.is_empty() {
            Self::build_geoarrow_schema(&schema, &geo_columns)
        } else {
            schema
        };

        let schema_finalized = geo_columns.is_empty();
        Ok(Self {
            inner,
            schema,
            geo_columns,
            schema_finalized,
        })
    }

    /// Detect columns that match the Databricks geometry struct pattern:
    /// `Struct<srid: Int32, wkb: Binary>`
    fn detect_geometry_columns(schema: &SchemaRef) -> Vec<usize> {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, field)| {
                if Self::is_geometry_struct(field.data_type()) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a DataType matches Databricks' geometry struct:
    /// `Struct<srid: Int32, wkb: Binary>`
    fn is_geometry_struct(dt: &DataType) -> bool {
        if let DataType::Struct(fields) = dt {
            if fields.len() == 2 {
                let has_srid = fields
                    .iter()
                    .any(|f| f.name() == "srid" && f.data_type() == &DataType::Int32);
                let has_wkb = fields
                    .iter()
                    .any(|f| f.name() == "wkb" && f.data_type() == &DataType::Binary);
                return has_srid && has_wkb;
            }
        }
        false
    }

    /// Build a GeoArrow-compatible schema by replacing geometry struct fields
    /// with Binary fields annotated with `ARROW:extension:name=geoarrow.wkb`.
    ///
    /// CRS metadata is added later from the first batch's SRID values.
    fn build_geoarrow_schema(schema: &SchemaRef, geo_columns: &[usize]) -> SchemaRef {
        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if geo_columns.contains(&i) {
                    let mut metadata = field.metadata().clone();
                    metadata.insert(
                        "ARROW:extension:name".to_string(),
                        "geoarrow.wkb".to_string(),
                    );
                    Field::new(field.name(), DataType::Binary, field.is_nullable())
                        .with_metadata(metadata)
                } else {
                    field.as_ref().clone()
                }
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }

    /// Update the schema with CRS metadata from the first batch's SRID values.
    /// Reads the srid field from each geometry struct column and encodes it as
    /// PROJJSON in `ARROW:extension:metadata`.
    fn finalize_schema_with_crs(&mut self, batch: &RecordBatch) {
        let mut new_fields: Vec<Field> = self.schema.fields().iter().map(|f| f.as_ref().clone()).collect();

        for &col_idx in &self.geo_columns {
            // The original batch still has the struct column
            let col = batch.column(col_idx);
            if let Some(struct_array) = col.as_any().downcast_ref::<StructArray>() {
                if let Some(srid_array) = struct_array.column_by_name("srid") {
                    if let Some(srid_arr) = srid_array.as_any().downcast_ref::<arrow_array::Int32Array>() {
                        // Read SRID from first non-null row
                        let srid = (0..srid_arr.len())
                            .find(|&i| !srid_arr.is_null(i))
                            .map(|i| srid_arr.value(i))
                            .unwrap_or(0);

                        if srid != 0 {
                            let crs_json = Self::srid_to_projjson(srid);
                            let mut metadata = new_fields[col_idx].metadata().clone();
                            metadata.insert(
                                "ARROW:extension:metadata".to_string(),
                                format!("{{\"crs\":{}}}", crs_json),
                            );
                            new_fields[col_idx] = new_fields[col_idx].clone().with_metadata(metadata);
                        }
                    }
                }
            }
        }

        self.schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            self.schema.metadata().clone(),
        ));
        self.schema_finalized = true;
    }

    /// Convert an SRID to a PROJJSON CRS string.
    fn srid_to_projjson(srid: i32) -> String {
        let name = match srid {
            4326 => "WGS 84",
            3857 => "WGS 84 / Pseudo-Mercator",
            _ => "Unknown",
        };
        format!(
            "{{\"type\":\"ProjectedCRS\",\"name\":\"{name}\",\"id\":{{\"authority\":\"EPSG\",\"code\":{srid}}}}}",
            name = name,
            srid = srid,
        )
    }

    /// Transform a RecordBatch: extract `wkb` child array from geometry struct
    /// columns. This is zero-copy — it just takes a reference to the child array.
    fn transform_batch_for_geoarrow(&self, batch: RecordBatch) -> RecordBatch {
        if self.geo_columns.is_empty() {
            return batch;
        }

        let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();

        for &col_idx in &self.geo_columns {
            let col = &columns[col_idx];
            if let Some(struct_array) = col.as_any().downcast_ref::<StructArray>() {
                if let Some(wkb_array) = struct_array.column_by_name("wkb") {
                    columns[col_idx] = wkb_array.clone();
                }
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns).unwrap_or(batch)
    }

    /// Augment an Arrow schema with Databricks column metadata from the manifest.
    fn augment_schema_with_manifest(schema: &SchemaRef, manifest: &ResultManifest) -> SchemaRef {
        let col_by_pos: HashMap<usize, &crate::types::sea::ColumnInfo> = manifest
            .schema
            .columns
            .iter()
            .filter_map(|c| usize::try_from(c.position).ok().map(|pos| (pos, c)))
            .collect();

        let new_fields: Vec<Field> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let col_info = col_by_pos.get(&i).copied();

                match col_info {
                    Some(info) => {
                        let mut metadata = field.metadata().clone();
                        metadata
                            .insert(metadata_keys::TYPE_NAME.to_string(), info.type_name.clone());
                        metadata
                            .insert(metadata_keys::TYPE_TEXT.to_string(), info.type_text.clone());
                        if let Some(prec) = info.type_precision {
                            metadata.insert(
                                metadata_keys::TYPE_PRECISION.to_string(),
                                prec.to_string(),
                            );
                        }
                        if let Some(scale) = info.type_scale {
                            metadata
                                .insert(metadata_keys::TYPE_SCALE.to_string(), scale.to_string());
                        }
                        if let Some(ref interval) = info.type_interval_type {
                            metadata.insert(
                                metadata_keys::TYPE_INTERVAL_TYPE.to_string(),
                                interval.clone(),
                            );
                        }
                        field.as_ref().clone().with_metadata(metadata)
                    }
                    None => field.as_ref().clone(),
                }
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ))
    }
}

impl arrow_array::RecordBatchReader for ResultReaderAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for ResultReaderAdapter {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next_batch() {
            Ok(Some(batch)) => {
                if !self.geo_columns.is_empty() {
                    // On first batch, capture SRID for CRS metadata
                    if !self.schema_finalized {
                        self.finalize_schema_with_crs(&batch);
                    }
                    Some(Ok(self.transform_batch_for_geoarrow(batch)))
                } else {
                    Some(Ok(batch))
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(ArrowError::ExternalError(Box::new(
                std::io::Error::other(e.to_string()),
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::test_utils::{ErrorReader, MockReader, SchemaErrorReader};
    use crate::types::sea::{ColumnInfo, ResultManifest, ResultSchema};
    use arrow_array::StringArray;

    fn make_test_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    // --- ResultReaderAdapter tests ---

    #[test]
    fn test_adapter_schema() {
        let batch = make_test_batch(vec!["a"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let adapter = ResultReaderAdapter::new(reader, None, false).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "name");
    }

    #[test]
    fn test_adapter_iterates_batches() {
        let batch1 = make_test_batch(vec!["a", "b"]);
        let batch2 = make_test_batch(vec!["c"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch1, batch2]));
        let mut adapter = ResultReaderAdapter::new(reader, None, false).unwrap();

        let first = adapter.next().unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);

        let second = adapter.next().unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);

        assert!(adapter.next().is_none());
    }

    #[test]
    fn test_adapter_empty_reader() {
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![]));
        let mut adapter = ResultReaderAdapter::new(reader, None, false).unwrap();
        assert!(adapter.next().is_none());
    }

    #[test]
    fn test_adapter_propagates_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let reader: Box<dyn ResultReader + Send> = Box::new(ErrorReader { schema });
        let mut adapter = ResultReaderAdapter::new(reader, None, false).unwrap();

        let result = adapter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_adapter_schema_error() {
        let reader: Box<dyn ResultReader + Send> = Box::new(SchemaErrorReader);
        let result = ResultReaderAdapter::new(reader, None, false);
        assert!(result.is_err());
    }

    // --- EmptyReader tests ---

    #[test]
    fn test_empty_reader_with_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut reader = EmptyReader::new(schema.clone());

        // Schema should be preserved
        let result_schema = reader.schema().unwrap();
        assert_eq!(result_schema.fields().len(), 2);
        assert_eq!(result_schema.field(0).name(), "id");
        assert_eq!(result_schema.field(1).name(), "name");

        // Should return no batches
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_empty_reader_with_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let mut reader = EmptyReader::new(schema);

        assert_eq!(reader.schema().unwrap().fields().len(), 0);
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_map_databricks_types() {
        assert_eq!(
            ResultReaderFactory::map_databricks_type("BOOLEAN"),
            DataType::Boolean
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("INT"),
            DataType::Int32
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("BIGINT"),
            DataType::Int64
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("STRING"),
            DataType::Utf8
        );
        assert_eq!(
            ResultReaderFactory::map_databricks_type("DOUBLE"),
            DataType::Float64
        );
        // Unknown types fall back to Utf8
        assert_eq!(
            ResultReaderFactory::map_databricks_type("UNKNOWN_TYPE"),
            DataType::Utf8
        );
    }

    // --- Schema augmentation tests ---

    fn make_manifest(columns: Vec<ColumnInfo>) -> ResultManifest {
        ResultManifest {
            format: "ARROW_STREAM".to_string(),
            schema: ResultSchema {
                column_count: columns.len() as i32,
                columns,
            },
            total_chunk_count: None,
            total_row_count: None,
            total_byte_count: None,
            truncated: false,
            chunks: None,
            result_compression: None,
        }
    }

    fn make_column_info(
        name: &str,
        type_name: &str,
        type_text: &str,
        position: i32,
        precision: Option<i64>,
        scale: Option<i64>,
        interval_type: Option<&str>,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            type_name: type_name.to_string(),
            type_text: type_text.to_string(),
            position,
            type_precision: precision,
            type_scale: scale,
            type_interval_type: interval_type.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_augment_schema_basic_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let manifest = make_manifest(vec![
            make_column_info("id", "INT", "INT", 0, None, None, None),
            make_column_info("name", "STRING", "STRING", 1, None, None, None),
            make_column_info("active", "BOOLEAN", "BOOLEAN", 2, None, None, None),
        ]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        assert_eq!(augmented.fields().len(), 3);
        // Check type_name and type_text are set
        assert_eq!(
            augmented.field(0).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"INT".to_string())
        );
        assert_eq!(
            augmented.field(0).metadata().get(metadata_keys::TYPE_TEXT),
            Some(&"INT".to_string())
        );
        assert_eq!(
            augmented.field(1).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"STRING".to_string())
        );
        // Precision/scale should not be present
        assert!(augmented
            .field(0)
            .metadata()
            .get(metadata_keys::TYPE_PRECISION)
            .is_none());
    }

    #[test]
    fn test_augment_schema_decimal_with_precision_scale() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let manifest = make_manifest(vec![make_column_info(
            "amount",
            "DECIMAL",
            "DECIMAL(10,2)",
            0,
            Some(10),
            Some(2),
            None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_NAME),
            Some(&"DECIMAL".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_TEXT),
            Some(&"DECIMAL(10,2)".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_PRECISION),
            Some(&"10".to_string())
        );
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_SCALE),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_augment_schema_interval_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration",
            DataType::Utf8,
            true,
        )]));
        let manifest = make_manifest(vec![make_column_info(
            "duration",
            "INTERVAL",
            "INTERVAL YEAR TO MONTH",
            0,
            None,
            None,
            Some("YEAR_MONTH"),
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_INTERVAL_TYPE),
            Some(&"YEAR_MONTH".to_string())
        );
    }

    #[test]
    fn test_augment_schema_missing_column_info() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("known", DataType::Int32, true),
            Field::new("unknown", DataType::Utf8, true),
        ]));
        // Only provide metadata for position 0, not position 1
        let manifest = make_manifest(vec![make_column_info(
            "known", "INT", "INT", 0, None, None, None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        // First field should have metadata
        assert!(augmented
            .field(0)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
        // Second field should have no databricks metadata
        assert!(!augmented
            .field(1)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
    }

    #[test]
    fn test_augment_schema_preserves_existing_metadata() {
        let mut existing_metadata = std::collections::HashMap::new();
        existing_metadata.insert("custom.key".to_string(), "custom_value".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("col", DataType::Int32, true).with_metadata(existing_metadata)
        ]));
        let manifest = make_manifest(vec![make_column_info(
            "col", "INT", "INT", 0, None, None, None,
        )]);

        let augmented = ResultReaderAdapter::augment_schema_with_manifest(&schema, &manifest);

        let field = augmented.field(0);
        // Existing metadata preserved
        assert_eq!(
            field.metadata().get("custom.key"),
            Some(&"custom_value".to_string())
        );
        // New metadata added
        assert_eq!(
            field.metadata().get(metadata_keys::TYPE_NAME),
            Some(&"INT".to_string())
        );
    }

    #[test]
    fn test_adapter_with_manifest() {
        let batch = make_test_batch(vec!["hello"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));
        let manifest = make_manifest(vec![make_column_info(
            "name", "STRING", "STRING", 0, None, None, None,
        )]);

        let adapter = ResultReaderAdapter::new(reader, Some(&manifest), false).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        assert_eq!(
            schema.field(0).metadata().get(metadata_keys::TYPE_NAME),
            Some(&"STRING".to_string())
        );
    }

    #[test]
    fn test_adapter_without_manifest() {
        let batch = make_test_batch(vec!["hello"]);
        let reader: Box<dyn ResultReader + Send> = Box::new(MockReader::new(vec![batch]));

        let adapter = ResultReaderAdapter::new(reader, None, false).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&adapter);
        // No databricks metadata when manifest is None
        assert!(!schema
            .field(0)
            .metadata()
            .contains_key(metadata_keys::TYPE_NAME));
    }
}
