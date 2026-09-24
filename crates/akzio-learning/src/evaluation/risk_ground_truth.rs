pub fn apply_risk_ground_truth_assessments(
    observations: &mut [GovernedHorizonObservation],
    schedule: &OutcomeSchedule,
    schedule_ref: &ArtifactRef,
    decision_producer_identity: &str,
    assessments: &[(ArtifactRef, RiskGroundTruthAssessment)],
    used_at: DateTime<Utc>,
) -> EvaluationRuntimeResult<()> {
    // 一个 horizon 只能绑定一个已封存的外部 RiskGroundTruthAssessment；assessment 由
    // 独立 reviewer 提供，本模块只检查身份、日期、集合子集关系并把引用挂到观察上。
    let mut seen = BTreeSet::new();
    for (assessment_ref, assessment) in assessments {
        if assessment_ref.kind != ArtifactKind::RiskGroundTruthAssessment
            || !seen.insert(assessment.horizon)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate risk ground truth assessment",
            ));
        }
        assessment.validate_sealed_at(used_at)?;
        if assessment.schedule != *schedule_ref
            || assessment.decision != schedule.decision
            || assessment.decision_producer_identity != decision_producer_identity
        {
            return Err(EvaluationError::InvalidMaterialization(
                "risk ground truth identity mismatch",
            ));
        }
        let observation = observations
            .iter_mut()
            .find(|observation| observation.horizon == assessment.horizon)
            .ok_or(EvaluationError::InvalidMaterialization(
                "risk ground truth horizon mismatch",
            ))?;
        if observation.observed_trading_day != assessment.observed_trading_day
            || observation.risk_recall.is_some()
        {
            return Err(EvaluationError::InvalidMaterialization(
                "risk ground truth observation mismatch",
            ));
        }
        let measurement = GovernedRiskRecall {
            assessment: assessment_ref.clone(),
            expected_risk_ids: assessment.expected_risk_ids.clone(),
            detected_risk_ids: assessment.detected_risk_ids.clone(),
        };
        measurement.validate()?;
        observation.risk_recall = Some(measurement);
    }
    Ok(())
}

impl EvaluationRuntime {
    /// Records one externally reviewed, sealed assessment. This method never
    /// generates reviewer/verifier identities or risk labels itself.
    pub fn record_risk_ground_truth_assessment_fenced(
        &self,
        lease: Option<&DaemonLease>,
        permit: &TaskWritePermit,
        assessment: &RiskGroundTruthAssessment,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Artifact> {
        // 记录前必须是 Paper，并验证 schedule/Decision producer/basis refs 的 CAS 身份；
        // 这里不生成 reviewer、风险标签或“默认正确”的测量值。
        self.require_paper(&permit.run_id)?;
        assessment.validate_sealed_at(now)?;
        let schedule_artifact = self.store.artifact(&assessment.schedule.artifact_id)?;
        let decision_artifact = self.store.artifact(&assessment.decision.artifact_id)?;
        if schedule_artifact.kind != ArtifactKind::OutcomeSchedule
            || decision_artifact.kind != ArtifactKind::Decision
            || assessment.decision_producer_identity != decision_artifact.producer
            || schedule_artifact.origin.as_ref().and_then(|origin| origin.run_id.as_ref())
                != Some(&permit.run_id)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "risk ground truth recording identity",
            ));
        }
        for basis_ref in &assessment.basis_refs {
            let basis = self.store.artifact(&basis_ref.artifact_id)?;
            if basis.kind != basis_ref.kind {
                return Err(EvaluationError::InvalidMaterialization(
                    "risk ground truth basis identity",
                ));
            }
        }
        for existing in self.store.artifacts_referencing(
            &assessment.schedule.artifact_id,
            Some(ArtifactKind::RiskGroundTruthAssessment),
        )? {
            let existing: RiskGroundTruthAssessment =
                serde_json::from_slice(&self.store.read_blob(&existing.blob)?)?;
            if existing.horizon == assessment.horizon {
                return Err(EvaluationError::InvalidMaterialization(
                    "duplicate risk ground truth assessment",
                ));
            }
        }
        let mut source_refs = vec![assessment.schedule.clone(), assessment.decision.clone()];
        source_refs.extend(assessment.basis_refs.iter().cloned());
        source_refs.sort();
        source_refs.dedup();
        let artifact = Artifact::new(
            ArtifactKind::RiskGroundTruthAssessment,
            self.store.stage_json(assessment)?,
            "akzio-learning.risk_ground_truth",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "independent_risk_review".to_owned(),
                observed_at: assessment.sealed_at,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            source_refs,
            now,
        )?;
        // write_task_artifact_fenced 把 permit/可选 lease、Artifact 和生命周期事件放入
        // 同一 Store 事务；成功返回才表示 assessment 已持久化，之前的内存对象不算记录。
        self.store.write_task_artifact_fenced(
            lease,
            permit,
            &artifact,
            akzio_domain::LifecycleEventType::RiskGroundTruthAssessmentRecorded,
            now,
        )?;
        Ok(artifact)
    }
}
