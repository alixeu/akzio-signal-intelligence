//! Stable identifiers that are not content-addressed artifacts.

id_type!(ExperienceId);
id_type!(OutcomeId);
id_type!(EvaluationId);
id_type!(PolicyTransitionId);
id_type!(LessonId);
id_type!(PaperCommitmentId);
id_type!(PaperRepriceId);
id_type!(ReconciliationId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_domain_ids_are_sixteen_lowercase_hex_characters() {
        for value in [OutcomeId::new().0, PolicyTransitionId::new().0] {
            assert_eq!(value.len(), 16);
            assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_eq!(value, value.to_ascii_lowercase());
        }
    }
}
