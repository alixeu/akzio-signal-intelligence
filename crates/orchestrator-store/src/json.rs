use std::{collections::BTreeMap, path::Path};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{Result, StoreError};

/// Serialize JSON with recursively sorted object keys and no insignificant
/// whitespace. Arrays intentionally preserve their supplied order.
pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonicalize(value)).map_err(|source| StoreError::JsonSerialize { source })
}

/// Hash bytes using the canonical `sha256:<lowercase-hex>` representation.
pub fn content_hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Hash an authoritative JSON document after removing its top-level
/// `content_hash` field.
pub fn content_hash(value: &Value) -> Result<String> {
    let mut unhashed = value.clone();
    let Some(object) = unhashed.as_object_mut() else {
        return Err(StoreError::ContentHashRequiresObject {
            path: "<in-memory JSON>".into(),
        });
    };
    object.remove("content_hash");
    Ok(content_hash_bytes(&canonical_json_bytes(&unhashed)?))
}

/// Return a cloned object with a fresh top-level content hash.
pub fn set_content_hash(value: &Value) -> Result<Value> {
    let mut hashed = canonicalized_value(value)?;
    let hash = content_hash(&hashed)?;
    let Some(object) = hashed.as_object_mut() else {
        return Err(StoreError::ContentHashRequiresObject {
            path: "<in-memory JSON>".into(),
        });
    };
    object.insert("content_hash".to_owned(), Value::String(hash));
    Ok(hashed)
}

/// Typed authoritative documents opt into hash sealing without exposing a
/// mutable JSON tree to callers.
pub trait ContentHashDocument: Serialize {
    fn content_hash(&self) -> &str;
    fn set_content_hash(&mut self, hash: String);
}

/// Recompute and install the top-level `content_hash` for a typed document.
pub fn seal_content_hash<T: ContentHashDocument>(mut document: T) -> Result<T> {
    document.set_content_hash(String::new());
    let value =
        serde_json::to_value(&document).map_err(|source| StoreError::JsonSerialize { source })?;
    document.set_content_hash(content_hash(&canonicalized_value(&value)?)?);
    Ok(document)
}

/// Validate a top-level authoritative content hash.
pub fn validate_content_hash(value: &Value) -> Result<()> {
    validate_content_hash_at(value, Path::new("<in-memory JSON>"))
}

/// Validate a top-level authoritative content hash and retain the disk path in
/// diagnostics when a reader knows it.
pub fn validate_content_hash_at(value: &Value, path: &Path) -> Result<()> {
    let Some(object) = value.as_object() else {
        return Err(StoreError::ContentHashRequiresObject {
            path: path.to_path_buf(),
        });
    };
    let Some(found) = object.get("content_hash").and_then(Value::as_str) else {
        return Err(StoreError::MissingContentHash {
            path: path.to_path_buf(),
        });
    };
    let expected = content_hash(value)?;
    if found == expected {
        Ok(())
    } else {
        Err(StoreError::ContentHashMismatch {
            path: path.to_path_buf(),
            expected,
            found: found.to_owned(),
        })
    }
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize(value)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        _ => value.clone(),
    }
}

/// Normalize a JSON value through the exact canonical representation emitted
/// by the FileStore. `serde_json::Value` can preserve an arbitrary-precision
/// number's original spelling in memory, while a subsequent write may emit a
/// shorter equivalent spelling. Hashing this round-tripped value keeps the
/// sealed document and its on-disk representation identical.
fn canonicalized_value(value: &Value) -> Result<Value> {
    let bytes = canonical_json_bytes(value)?;
    serde_json::from_slice(&bytes).map_err(|source| StoreError::JsonSerialize { source })
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{canonical_json_bytes, set_content_hash, validate_content_hash};

    #[test]
    fn canonical_json_sorts_recursively_but_preserves_arrays() {
        let value = json!({"z": {"b": 1, "a": 2}, "a": [ {"d": 4, "c": 3}, 2 ]});
        let bytes = canonical_json_bytes(&value).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            r#"{"a":[{"c":3,"d":4},2],"z":{"a":2,"b":1}}"#
        );
    }

    #[test]
    fn content_hash_is_stable_and_detects_tampering() {
        let value = set_content_hash(&json!({"schema_version": 1, "b": 2, "a": 1})).unwrap();
        validate_content_hash(&value).unwrap();

        let mut tampered = value;
        tampered["a"] = json!(3);
        assert!(validate_content_hash(&tampered).is_err());
    }

    #[test]
    fn content_hash_survives_float_normalization_on_write() {
        let value = set_content_hash(&json!({
            "schema_version": 1,
            "computed": 0.37939999999999996
        }))
        .unwrap();
        let bytes = canonical_json_bytes(&value).unwrap();
        let persisted: Value = serde_json::from_slice(&bytes).unwrap();
        validate_content_hash(&persisted).unwrap();
    }
}
