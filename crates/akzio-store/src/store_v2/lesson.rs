use super::*;

use akzio_domain::{Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, LessonOrigin};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLesson {
    pub artifact: Artifact,
    pub lesson: Lesson,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LessonWriteResult {
    pub lesson: StoredLesson,
    pub newly_created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LessonUsage {
    pub context_manifests: u64,
    pub decision_contexts: u64,
    pub latest_used_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
#[path = "lesson_tests.rs"]
mod tests;

include!("lesson_parts/write.rs");
include!("lesson_parts/queries_verify.rs");
include!("lesson_parts/helpers.rs");
