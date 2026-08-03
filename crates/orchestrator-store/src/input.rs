//! Per-run immutable copies of market-data inputs captured from readable stable paths.
//!
//! Technical and Jin10 source files are updated atomically. A run manifest
//! records the exact hash it started with and stores the corresponding bytes
//! below the run. Consumers never reopen the mutable source after capture.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    content_hash_bytes, validate_relative_path, ContentHashDocument, FileSchemaKind, FileStore,
    Result, RunLocation, StoreError, Versioned,
};

pub const DATA_FILE_METADATA_SCHEMA_VERSION: u32 = 1;
pub const INPUT_SNAPSHOT_MANIFEST_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    Technical,
    Jin10,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Jin10Format {
    Csv,
    Jsonl,
}

impl Jin10Format {
    fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Jsonl => "jsonl",
        }
    }
}

/// A Rust-owned input identity.  Paths are always derived from this typed
/// source; neither a model nor a caller can provide an arbitrary file path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InputSource {
    Technical {
        ticker: String,
        interval: String,
    },
    Jin10 {
        workflow_date: String,
        format: Jin10Format,
    },
}

impl InputSource {
    pub fn technical(ticker: impl Into<String>, interval: impl Into<String>) -> Result<Self> {
        let source = Self::Technical {
            ticker: ticker.into(),
            interval: interval.into(),
        };
        source.validate()?;
        Ok(source)
    }

    pub fn jin10(workflow_date: impl Into<String>, format: Jin10Format) -> Result<Self> {
        let source = Self::Jin10 {
            workflow_date: workflow_date.into(),
            format,
        };
        source.validate()?;
        Ok(source)
    }

    pub fn kind(&self) -> InputKind {
        match self {
            Self::Technical { .. } => InputKind::Technical,
            Self::Jin10 { .. } => InputKind::Jin10,
        }
    }

    /// The stable, human-readable path of the source payload.
    pub fn payload_relative_path(&self) -> Result<PathBuf> {
        self.validate()?;
        match self {
            Self::Technical { ticker, interval } => Ok(PathBuf::from("data")
                .join("technical")
                .join(readable_component("ticker", ticker)?)
                .join(format!(
                    "{}.csv",
                    orchestrator_core::interval_file_label(interval).ok_or_else(|| {
                        invalid("technical input source", "interval is not supported")
                    })?
                ))),
            Self::Jin10 {
                workflow_date,
                format,
            } => Ok(PathBuf::from("data")
                .join("jin10")
                .join(format!("{workflow_date}.{}", format.extension()))),
        }
    }

    pub fn metadata_relative_path(&self) -> Result<PathBuf> {
        let payload = self.payload_relative_path()?;
        let name = payload
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid("input source", "payload path is not valid UTF-8"))?;
        Ok(payload.with_file_name(format!("{name}.metadata.json")))
    }

    fn stable_key(&self) -> String {
        match self {
            Self::Technical { ticker, interval } => format!("technical\0{ticker}\0{interval}"),
            Self::Jin10 {
                workflow_date,
                format,
            } => format!("jin10\0{workflow_date}\0{}", format.extension()),
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Technical { ticker, interval } => {
                if ticker.trim().is_empty() || interval.trim().is_empty() {
                    return Err(invalid(
                        "technical input source",
                        "ticker and interval must not be empty",
                    ));
                }
            }
            Self::Jin10 { workflow_date, .. } if !is_workflow_date(workflow_date) => {
                return Err(invalid(
                    "jin10 input source",
                    "workflow_date must be a valid YYYY-MM-DD date",
                ));
            }
            Self::Jin10 { .. } => {}
        }
        Ok(())
    }
}

/// Hash-sealed metadata accompanying each mutable source payload.  The raw
/// payload is not JSON, so `payload_hash` is its authoritative integrity
/// value while `content_hash` protects this schema document itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileMetadata {
    pub schema_version: u32,
    pub source: InputSource,
    pub payload_relative_path: String,
    pub payload_hash: String,
    pub payload_bytes: u64,
    pub written_at: String,
    pub content_hash: String,
}

impl DataFileMetadata {
    fn new(source: InputSource, payload: &[u8], written_at: String) -> Result<Self> {
        source.validate()?;
        if written_at.is_empty() {
            return Err(invalid(
                "data file metadata",
                "written_at must not be empty",
            ));
        }
        let payload_relative_path = path_string(&source.payload_relative_path()?)?;
        Ok(Self {
            schema_version: DATA_FILE_METADATA_SCHEMA_VERSION,
            source,
            payload_relative_path,
            payload_hash: content_hash_bytes(payload),
            payload_bytes: u64::try_from(payload.len())
                .map_err(|_| invalid("data file metadata", "payload length exceeds u64"))?,
            written_at,
            content_hash: String::new(),
        })
    }

    fn validate_for_source(&self, source: &InputSource) -> Result<()> {
        source.validate()?;
        if self.schema_version != DATA_FILE_METADATA_SCHEMA_VERSION {
            return Err(invalid(
                "data file metadata",
                "schema_version differs from the typed metadata version",
            ));
        }
        if &self.source != source {
            return Err(invalid(
                "data file metadata",
                "metadata source differs from the requested input source",
            ));
        }
        let expected_path = source.payload_relative_path()?;
        let stored_path = Path::new(&self.payload_relative_path);
        validate_relative_path(stored_path)?;
        if stored_path != expected_path {
            return Err(invalid(
                "data file metadata",
                "payload_relative_path differs from its Rust-owned source path",
            ));
        }
        if !is_sha256_hash(&self.payload_hash) {
            return Err(invalid(
                "data file metadata",
                "payload_hash must be a sha256 hash",
            ));
        }
        if self.written_at.is_empty() {
            return Err(invalid(
                "data file metadata",
                "written_at must not be empty",
            ));
        }
        Ok(())
    }
}

impl Versioned for DataFileMetadata {
    const SCHEMA_VERSION: u32 = DATA_FILE_METADATA_SCHEMA_VERSION;
}

impl ContentHashDocument for DataFileMetadata {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSnapshot {
    pub source: InputSource,
    /// Immutable payload copied beneath this run.
    pub payload_relative_path: String,
    /// Mutable source path retained only as capture provenance.
    pub source_payload_relative_path: String,
    pub source_metadata_relative_path: String,
    pub source_metadata_hash: String,
    pub source_payload_hash: String,
    pub payload_bytes: u64,
}

impl InputSnapshot {
    fn from_metadata(
        source: InputSource,
        metadata: &DataFileMetadata,
        location: &RunLocation,
    ) -> Result<Self> {
        metadata.validate_for_source(&source)?;
        Ok(Self {
            source: source.clone(),
            payload_relative_path: path_string(&run_payload_relative_path(location, &source)?)?,
            source_payload_relative_path: path_string(&source.payload_relative_path()?)?,
            source_metadata_relative_path: path_string(&source.metadata_relative_path()?)?,
            source_metadata_hash: metadata.content_hash.clone(),
            source_payload_hash: metadata.payload_hash.clone(),
            payload_bytes: metadata.payload_bytes,
        })
    }

    fn validate_for_location(&self, location: &RunLocation) -> Result<()> {
        self.source.validate()?;
        let expected_payload = run_payload_relative_path(location, &self.source)?;
        let expected_source_payload = self.source.payload_relative_path()?;
        let expected_metadata = self.source.metadata_relative_path()?;
        validate_relative_path(Path::new(&self.payload_relative_path))?;
        validate_relative_path(Path::new(&self.source_payload_relative_path))?;
        validate_relative_path(Path::new(&self.source_metadata_relative_path))?;
        if Path::new(&self.payload_relative_path) != expected_payload
            || Path::new(&self.source_payload_relative_path) != expected_source_payload
            || Path::new(&self.source_metadata_relative_path) != expected_metadata
        {
            return Err(invalid(
                "input snapshot",
                "stored paths differ from Rust-owned input paths",
            ));
        }
        if !is_sha256_hash(&self.source_metadata_hash) || !is_sha256_hash(&self.source_payload_hash)
        {
            return Err(invalid(
                "input snapshot",
                "stored hashes must be sha256 hashes",
            ));
        }
        Ok(())
    }
}

/// The authority that binds a run to the hashes of its stable input files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputSnapshotManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub current_date: String,
    pub created_at: String,
    pub inputs: Vec<InputSnapshot>,
    pub content_hash: String,
}

impl InputSnapshotManifest {
    fn new(location: &RunLocation, created_at: String, inputs: Vec<InputSnapshot>) -> Result<Self> {
        if created_at.is_empty() {
            return Err(invalid(
                "input snapshot manifest",
                "created_at must not be empty",
            ));
        }
        let manifest = Self {
            schema_version: INPUT_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            run_id: location.run_id.clone(),
            current_date: location.current_date.clone(),
            created_at,
            inputs,
            content_hash: String::new(),
        };
        manifest.validate_for_location(location)?;
        Ok(manifest)
    }

    pub fn relative_path(location: &RunLocation) -> Result<PathBuf> {
        location.child_relative(Path::new("inputs/manifest.json"))
    }

    fn validate_for_location(&self, location: &RunLocation) -> Result<()> {
        if self.schema_version != INPUT_SNAPSHOT_MANIFEST_SCHEMA_VERSION {
            return Err(invalid(
                "input snapshot manifest",
                "schema_version differs from the typed manifest version",
            ));
        }
        if self.run_id != location.run_id || self.current_date != location.current_date {
            return Err(invalid(
                "input snapshot manifest",
                "manifest run identity differs from its store location",
            ));
        }
        if self.created_at.is_empty() {
            return Err(invalid(
                "input snapshot manifest",
                "created_at must not be empty",
            ));
        }
        let mut seen = BTreeSet::new();
        for snapshot in &self.inputs {
            snapshot.validate_for_location(location)?;
            if !seen.insert(snapshot.source.stable_key()) {
                return Err(invalid(
                    "input snapshot manifest",
                    "each input source may appear only once",
                ));
            }
        }
        Ok(())
    }

    fn validate_requested_sources(&self, sources: &[InputSource]) -> Result<()> {
        let requested = source_keys(sources)?;
        let existing = self
            .inputs
            .iter()
            .map(|snapshot| snapshot.source.stable_key())
            .collect::<BTreeSet<_>>();
        if existing != requested {
            return Err(invalid(
                "input snapshot manifest",
                "existing run input set differs from the requested source set",
            ));
        }
        Ok(())
    }
}

impl Versioned for InputSnapshotManifest {
    const SCHEMA_VERSION: u32 = INPUT_SNAPSHOT_MANIFEST_SCHEMA_VERSION;
}

impl ContentHashDocument for InputSnapshotManifest {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

/// Atomically write a mutable Technical/Jin10 source and then publish its
/// hash-sealed metadata.  A crash between the two is safe: readers hard-fail
/// on the payload/metadata hash mismatch instead of accepting mixed data.
pub fn write_input_payload(
    store: &FileStore,
    source: InputSource,
    payload: &[u8],
    written_at: impl Into<String>,
) -> Result<DataFileMetadata> {
    source.validate()?;
    let metadata = DataFileMetadata::new(source.clone(), payload, written_at.into())?;
    let payload_relative = source.payload_relative_path()?;
    let metadata_relative = source.metadata_relative_path()?;
    store.write_bytes(&payload_relative, payload)?;
    store.write_authoritative_json(&metadata_relative, metadata)
}

pub fn read_input_metadata(store: &FileStore, source: &InputSource) -> Result<DataFileMetadata> {
    source.validate()?;
    let metadata = store.read_versioned_json::<DataFileMetadata>(
        &source.metadata_relative_path()?,
        FileSchemaKind::DataFileMetadata,
    )?;
    metadata.validate_for_source(source)?;
    Ok(metadata)
}

/// Read a mutable source only after verifying its raw bytes against the
/// matching hash-sealed metadata.  Workflow consumers should call
/// [`read_snapshotted_input`] after capture instead.
pub fn read_input_payload(store: &FileStore, source: &InputSource) -> Result<Vec<u8>> {
    let metadata = read_input_metadata(store, source)?;
    let payload = store.read_bytes(&source.payload_relative_path()?)?;
    verify_payload(
        "input payload",
        &source.payload_relative_path()?,
        &payload,
        &metadata.payload_hash,
        metadata.payload_bytes,
    )?;
    Ok(payload)
}

/// Copy the current source bytes beneath this run and bind the manifest to the copy.
/// If a manifest already exists it is reused only when the requested source
/// identities match exactly.
pub fn capture_run_inputs(
    store: &FileStore,
    location: &RunLocation,
    sources: &[InputSource],
    created_at: impl Into<String>,
) -> Result<InputSnapshotManifest> {
    source_keys(sources)?;
    let manifest_relative = InputSnapshotManifest::relative_path(location)?;
    if store.exists(&manifest_relative)? {
        let manifest = read_input_snapshot_manifest(store, location)?;
        manifest.validate_requested_sources(sources)?;
        return Ok(manifest);
    }

    let sorted_sources = sorted_sources(sources)?;
    let mut snapshots = Vec::with_capacity(sorted_sources.len());
    for source in sorted_sources {
        let metadata = read_input_metadata(store, &source)?;
        let payload = store.read_bytes(&source.payload_relative_path()?)?;
        verify_payload(
            "input payload",
            &source.payload_relative_path()?,
            &payload,
            &metadata.payload_hash,
            metadata.payload_bytes,
        )?;
        let snapshot = InputSnapshot::from_metadata(source, &metadata, location)?;
        store.write_bytes(Path::new(&snapshot.payload_relative_path), &payload)?;
        snapshots.push(snapshot);
    }

    let manifest = InputSnapshotManifest::new(location, created_at.into(), snapshots)?;
    store.write_authoritative_json(&manifest_relative, manifest)
}

pub fn read_input_snapshot_manifest(
    store: &FileStore,
    location: &RunLocation,
) -> Result<InputSnapshotManifest> {
    let manifest = store.read_versioned_json::<InputSnapshotManifest>(
        &InputSnapshotManifest::relative_path(location)?,
        FileSchemaKind::InputSnapshotManifest,
    )?;
    manifest.validate_for_location(location)?;
    Ok(manifest)
}

/// Read the immutable run-local copy and verify it against the run manifest.
pub fn read_snapshotted_input(
    store: &FileStore,
    location: &RunLocation,
    source: &InputSource,
) -> Result<Vec<u8>> {
    source.validate()?;
    let manifest = read_input_snapshot_manifest(store, location)?;
    let snapshot = manifest
        .inputs
        .iter()
        .find(|snapshot| &snapshot.source == source)
        .ok_or_else(|| {
            invalid(
                "input snapshot manifest",
                "requested source was not captured",
            )
        })?;
    let payload = store.read_bytes(Path::new(&snapshot.payload_relative_path))?;
    verify_payload(
        "bound input",
        Path::new(&snapshot.payload_relative_path),
        &payload,
        &snapshot.source_payload_hash,
        snapshot.payload_bytes,
    )?;
    Ok(payload)
}

fn run_payload_relative_path(location: &RunLocation, source: &InputSource) -> Result<PathBuf> {
    let source_path = source.payload_relative_path()?;
    let suffix = source_path.strip_prefix("data").map_err(|_| {
        invalid(
            "input source",
            "payload path must remain beneath the data directory",
        )
    })?;
    location.child_relative(&Path::new("inputs/payloads").join(suffix))
}

fn sorted_sources(sources: &[InputSource]) -> Result<Vec<InputSource>> {
    source_keys(sources)?;
    let mut sorted = sources.to_vec();
    sorted.sort_by_key(InputSource::stable_key);
    Ok(sorted)
}

fn source_keys(sources: &[InputSource]) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for source in sources {
        source.validate()?;
        if !keys.insert(source.stable_key()) {
            return Err(invalid(
                "input snapshot manifest",
                "input source is duplicated",
            ));
        }
    }
    Ok(keys)
}

fn verify_payload(
    kind: &'static str,
    relative: &Path,
    payload: &[u8],
    expected_hash: &str,
    expected_bytes: u64,
) -> Result<()> {
    if expected_hash != content_hash_bytes(payload)
        || expected_bytes != u64::try_from(payload.len()).unwrap_or(u64::MAX)
    {
        return Err(invalid(
            kind,
            format!(
                "payload at {} does not match its authoritative metadata",
                relative.display()
            ),
        ));
    }
    Ok(())
}

fn path_string(path: &Path) -> Result<String> {
    validate_relative_path(path)?;
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("input path", "path must be valid UTF-8"))
}

fn readable_component(kind: &'static str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid(
            "input source",
            format!("{kind} must contain only letters, numbers, dash, underscore, or dot"),
        ));
    }
    Ok(value.to_ascii_lowercase())
}

fn invalid(kind: &'static str, message: impl Into<String>) -> StoreError {
    StoreError::InvalidDocument {
        kind,
        message: message.into(),
    }
}

fn is_sha256_hash(hash: &str) -> bool {
    let Some(hex) = hash.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_workflow_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
    {
        return false;
    }
    let Ok(year) = value[0..4].parse::<u32>() else {
        return false;
    };
    let Ok(month) = value[5..7].parse::<u32>() else {
        return false;
    };
    let Ok(day) = value[8..10].parse::<u32>() else {
        return false;
    };
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&day)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        capture_run_inputs, read_input_metadata, read_input_payload, read_input_snapshot_manifest,
        read_snapshotted_input, write_input_payload, InputSnapshotManifest, InputSource,
        Jin10Format,
    };
    use crate::{
        set_content_hash, FileSchemaKind, FileStore, FileStoreOptions, RunLocation, StoreError,
    };

    fn store() -> (tempfile::TempDir, FileStore) {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        (directory, store)
    }

    fn location() -> RunLocation {
        RunLocation::new("2026-07-27", "run:QQQ/one").unwrap()
    }

    #[test]
    fn technical_source_uses_a_readable_lowercase_path() {
        let (_directory, store) = store();
        let source = InputSource::technical("QQQ", "20min").unwrap();
        let metadata =
            write_input_payload(&store, source.clone(), b"first", "2026-07-27T00:00:00Z").unwrap();

        assert_eq!(read_input_payload(&store, &source).unwrap(), b"first");
        assert_eq!(read_input_metadata(&store, &source).unwrap(), metadata);
        let path = source.payload_relative_path().unwrap();
        assert_eq!(path, Path::new("data/technical/qqq/20min.csv"));
    }

    #[test]
    fn run_snapshot_survives_a_source_change_after_capture() {
        let (_directory, store) = store();
        let source = InputSource::technical("QQQ", "daily").unwrap();
        write_input_payload(&store, source.clone(), b"old", "2026-07-27T00:00:00Z").unwrap();
        let manifest = capture_run_inputs(
            &store,
            &location(),
            std::slice::from_ref(&source),
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        assert_eq!(manifest.inputs.len(), 1);
        let run_payload = Path::new(&manifest.inputs[0].payload_relative_path);
        assert!(store.exists(run_payload).unwrap());
        assert_ne!(run_payload, source.payload_relative_path().unwrap());

        write_input_payload(&store, source.clone(), b"new", "2026-07-27T00:02:00Z").unwrap();
        assert_eq!(read_input_payload(&store, &source).unwrap(), b"new");
        assert_eq!(
            read_snapshotted_input(&store, &location(), &source).unwrap(),
            b"old"
        );
    }

    #[test]
    fn source_or_snapshot_tampering_hard_fails_instead_of_using_changed_bytes() {
        let (_directory, store) = store();
        let source = InputSource::jin10("2026-07-27", Jin10Format::Jsonl).unwrap();
        write_input_payload(
            &store,
            source.clone(),
            b"{\"id\":1}\n",
            "2026-07-27T00:00:00Z",
        )
        .unwrap();
        let manifest = capture_run_inputs(
            &store,
            &location(),
            std::slice::from_ref(&source),
            "2026-07-27T00:01:00Z",
        )
        .unwrap();

        store
            .write_bytes(&source.payload_relative_path().unwrap(), b"mutated")
            .unwrap();
        assert!(read_input_payload(&store, &source).is_err());

        assert_eq!(
            read_snapshotted_input(&store, &location(), &source).unwrap(),
            b"{\"id\":1}\n"
        );
        store
            .write_bytes(
                Path::new(&manifest.inputs[0].payload_relative_path),
                b"mutated snapshot",
            )
            .unwrap();
        assert!(read_snapshotted_input(&store, &location(), &source).is_err());
    }

    #[test]
    fn existing_manifest_is_reused_only_for_the_same_source_set() {
        let (_directory, store) = store();
        let technical = InputSource::technical("QQQ", "daily").unwrap();
        let jin10 = InputSource::jin10("2026-07-27", Jin10Format::Csv).unwrap();
        write_input_payload(&store, technical.clone(), b"bars", "2026-07-27T00:00:00Z").unwrap();
        write_input_payload(&store, jin10.clone(), b"news", "2026-07-27T00:00:00Z").unwrap();
        let first = capture_run_inputs(
            &store,
            &location(),
            &[technical.clone(), jin10.clone()],
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        let resumed = capture_run_inputs(
            &store,
            &location(),
            &[jin10, technical],
            "ignored-on-resume",
        )
        .unwrap();
        assert_eq!(first, resumed);

        let different = InputSource::technical("SOXX", "daily").unwrap();
        assert!(
            capture_run_inputs(&store, &location(), &[different], "2026-07-27T00:03:00Z").is_err()
        );
    }

    #[test]
    fn snapshot_manifest_refuses_future_or_old_versions() {
        let (_directory, store) = store();
        let relative = InputSnapshotManifest::relative_path(&location()).unwrap();
        let future = set_content_hash(&json!({
            "schema_version": 4,
            "run_id": "run:QQQ/one",
            "current_date": "2026-07-27",
            "created_at": "now",
            "inputs": []
        }))
        .unwrap();
        store.write_json(&relative, &future).unwrap();
        assert!(matches!(
            store.read_versioned_json::<InputSnapshotManifest>(
                &relative,
                FileSchemaKind::InputSnapshotManifest
            ),
            Err(StoreError::UnsupportedFutureSchema { .. })
        ));

        let old = set_content_hash(&json!({
            "schema_version": 0,
            "run_id": "run:QQQ/one",
            "current_date": "2026-07-27",
            "created_at": "now",
            "inputs": []
        }))
        .unwrap();
        store.write_json(&relative, &old).unwrap();
        assert!(matches!(
            read_input_snapshot_manifest(&store, &location()),
            Err(StoreError::InvalidSchemaVersion { .. })
        ));
    }
}
