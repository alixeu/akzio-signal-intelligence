//! Derives the observational Lesson evidence ledger from sealed Paper outcomes.
//!
//! This module only *describes* what happened: which Lesson revision a decision
//! cited, and how that decision's sealed outcome closed. It makes no causal
//! claim, and nothing here may drive an automatic contest or retire. See
//! `LessonEvidence` in `akzio-domain` for why: utility after a Lesson was applied
//! moves with market regime, the other injected Lessons, the contract, the
//! topology, the target weights, execution cost and evidence quality, and the
//! outcome after a *rejected* Lesson is not a counterfactual. Attributing effect
//! needs a paired on/off comparison against the same outcome.

use chrono::{DateTime, Utc};

use akzio_domain::{
    ArtifactKind, ArtifactRef, DecisionContext, Lesson, LessonAttribution, LessonEvidence, Outcome,
    OutcomeHorizon, DOMAIN_SCHEMA_VERSION,
};

use crate::{EvaluationError, EvaluationRuntime, EvaluationRuntimeResult};

// 文件导读：LessonEvidence 是“决策引用了什么、最终 Outcome 如何关闭”的观察账本，
// 不是因果证明；Applied/Rejected 都保留，拒绝 Lesson 的 Outcome 也不是反事实收益。

impl EvaluationRuntime {
    /// Build one record per Lesson the decision took a position on.
    ///
    /// The upstream DecisionGate requires attribution for each manifest Lesson;
    /// this public helper only reads the supplied DecisionContext, so it does
    /// not re-prove that manifest closure or the supplied Outcome's CAS origin.
    /// Experience and CandidatePolicy references are skipped, not Lessons.
    // 本方法只从传入且经 context.validate 的 Applied/Rejected Lesson 引用构造观察记录；
    // context 本身不在此与 Store 正文比对。Outcome 阶段应调用下方按 ArtifactRef
    // 回读 Store 的入口，避免把调用方内存中的 context 当成已持久化事实。
    pub fn lesson_evidence_from_decision(
        &self,
        decision_context: &ArtifactRef,
        context: &DecisionContext,
        outcome_artifact: &ArtifactRef,
        outcome: &Outcome,
        recorded_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Vec<LessonEvidence>> {
        // 只有 sealed Outcome 才能同时提供 T+1/T+3/T+5 utility；Lesson 的应用/拒绝归因
        // 已由 DecisionContext 强制写入，因此这里按 manifest 中的 Lesson 引用逐条读取。
        context.validate()?;
        // Only a sealed outcome carries all three windows, and canonical
        // learning is defined on sealed outcomes alone.
        outcome.validate_sealed()?;
        if decision_context.kind != ArtifactKind::DecisionContext
            || outcome_artifact.kind != ArtifactKind::Outcome
        {
            return Err(EvaluationError::InvalidMaterialization(
                "lesson evidence reference kind",
            ));
        }

        let utility_ppm_by_horizon = Self::by_horizon(outcome, |window| window.utility_ppm, 0);
        // Calibration is a cohort statistic, not a property of one sealed
        // decision. The Outcome reference retained on this record is the
        // explicit provenance path to each horizon's ForecastScore; an
        // aggregate report may consume those scores after the policy minimum
        // sample count is met. Until then the measured state is `None` rather
        // than a fabricated single-event calibration value.
        let calibration_ppm_by_horizon = [None; 3];

        let mut records = Vec::new();
        // 固定遍历 Applied 和 Rejected 两组；filter 只保留 Lesson kind，循环实际消费引用，
        // 其他 kind（如 Experience/CandidatePolicy）不会被记为 lesson evidence。
        for (attribution, references) in [
            (LessonAttribution::Applied, &context.applied_learning_refs),
            (LessonAttribution::Rejected, &context.rejected_learning_refs),
        ] {
            for reference in references
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Lesson)
            {
                // read_lesson 再次从 Store 验证 kind、BLOB 与 Lesson schema；Experience 或
                // CandidatePolicy 虽然也有 outcome lineage，但不属于 Lesson evidence。
                let lesson = self.read_lesson(reference)?;
                let record = LessonEvidence {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    lesson_id: lesson.lesson_id,
                    lesson_artifact: reference.clone(),
                    decision_context: decision_context.clone(),
                    outcome: outcome_artifact.clone(),
                    attribution,
                    utility_ppm_by_horizon,
                    calibration_ppm_by_horizon,
                    recorded_at,
                };
                record.validate()?;
                records.push(record);
            }
        }
        Ok(records)
    }

    pub(crate) fn lesson_evidence_for_outcome(
        &self,
        decision_context: &ArtifactRef,
        outcome_artifact: &ArtifactRef,
        outcome: &Outcome,
        recorded_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Vec<LessonEvidence>> {
        // 只收 DecisionContext 引用而不收正文，避免调用者传入未持久化替身；Store/JSON/domain
        // 任一步错误经 ? 返回，上层不会拿不可信上下文继续评估。
        // Outcome 评估阶段只传 DecisionContext 的 ArtifactRef；正文从同一 Store 读取，避免
        // 调用者用未持久化的 context 替换正式 provenance。
        let artifact = self.store.artifact(&decision_context.artifact_id)?;
        if artifact.kind != ArtifactKind::DecisionContext {
            return Err(EvaluationError::InvalidMaterialization(
                "lesson evidence reference kind",
            ));
        }
        let context: DecisionContext =
            serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        self.lesson_evidence_from_decision(
            decision_context,
            &context,
            outcome_artifact,
            outcome,
            recorded_at,
        )
    }

    fn by_horizon<T: Copy>(
        outcome: &Outcome,
        project: impl Fn(&akzio_domain::OutcomeWindow) -> T,
        fallback: T,
    ) -> [T; 3] {
        // T: Copy 允许将 fallback 写入三个固定槽位；project 是静态分发的 Fn 闭包，
        // 按窗口映射数值且只借用 outcome，不会捕获或改变 Outcome 状态。
        // 固定数组槽位与 OutcomeHorizon::ALL 对齐；缺失窗口使用调用方给定 fallback，
        // 但 sealed 校验会在上层拒绝不完整 Outcome。
        let mut values = [fallback; 3];
        for (index, horizon) in OutcomeHorizon::ALL.into_iter().enumerate() {
            if let Some(window) = outcome
                .windows
                .iter()
                .find(|window| window.horizon == horizon)
            {
                values[index] = project(window);
            }
        }
        values
    }

    // 从 Store 取 Artifact、读 BLOB、反序列化并执行 Lesson::validate；任何一步 Err
    // 都向创建 evidence 的调用者传播，只有校验完整的 Lesson 才被返回。
    fn read_lesson(&self, reference: &ArtifactRef) -> EvaluationRuntimeResult<Lesson> {
        // Lesson 内容必须来自 Store/CAS 且自身通过 domain validate；仅有一个 kind 正确的
        // 引用不能绕过 BLOB 或 schema 校验。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(EvaluationError::InvalidMaterialization(
                "lesson evidence reference kind",
            ));
        }
        let lesson: Lesson = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        lesson.validate()?;
        Ok(lesson)
    }
}
