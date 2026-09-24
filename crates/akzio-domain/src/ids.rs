// 文件导读：声明不属于内容寻址 Artifact 的稳定业务标识类型。
//! Stable identifiers that are not content-addressed artifacts.

// 每个类型都由 lib.rs 中的宏展开为透明的 String 包装和统一的 ID 行为。
id_type!(ExperienceId);
id_type!(OutcomeId);
id_type!(EvaluationId);
id_type!(PolicyTransitionId);
id_type!(LessonId);
id_type!(PaperCommitmentId);
id_type!(PaperCancelId);
id_type!(PaperRepriceId);
id_type!(ReconciliationId);
