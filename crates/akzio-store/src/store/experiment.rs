// 文件导读：实验 Trial 和 SearchBiasCertificate 以 canonical Artifact 保存；本模块被 Canary
// staging 与 Doctor 调用，按 PolicySubject 筛选或重建不可变 trial ledger，不负责计算实验结论。
// 先读 experiment_trial_ledger 理解“全量恢复后按 typed subject 过滤”，再读 verify_experiment_history
// 理解完整性检查；连接和 Artifact payload 都由现有 Store 读取路径提供。
use super::*;

impl Store {
    // 输入先通过领域 subject 校验；查询会解码并校验全部 ExperimentTrial，再按 subject 精确过滤，
    // 用 created_at 后接 Artifact ID 稳定排序，最后消费内部 Artifact Vec 返回仅含引用和 payload 的 Vec。
    // 任一 unrelated trial 损坏也会使整次 ledger 失败；这个只读入口不会创建候选或激活 policy。
    // 多个 SQL/Artifact reads 共用连接但未显式开启 Deferred transaction，跨实例并发写入不构成同一快照。
    pub fn experiment_trial_ledger(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<(ArtifactRef, ExperimentTrial)>> {
        subject.validate()?;
        let connection = self.connection()?;
        let mut ledger = read_kind_artifacts(&connection, ArtifactKind::ExperimentTrial)?
            .into_iter()
            .map(|artifact| {
                let trial: ExperimentTrial =
                    self.read_artifact_payload_with_connection(&connection, &artifact)?;
                trial.validate()?;
                Ok((artifact, trial))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        ledger.retain(|(_, trial)| &trial.subject == subject);
        ledger.sort_by(|(left_artifact, left), (right_artifact, right)| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left_artifact.artifact_id.cmp(&right_artifact.artifact_id))
        });
        Ok(ledger
            .into_iter()
            .map(|(artifact, trial)| {
                (
                    ArtifactRef {
                        artifact_id: artifact.artifact_id,
                        kind: ArtifactKind::ExperimentTrial,
                    },
                    trial,
                )
            })
            .collect())
    }

    // Doctor 使用已持有连接逐项检查所有 trial/certificate：要求 canonical lifecycle、领域 payload
    // 有效，并使 Trial.source_refs 或 Certificate.trial_refs 与 Artifact.source_refs 完全相等。
    // 注意：当前 payload 调用走 `self.read_artifact_payload` → `self.read_blob`，
    // 非空 ledger 时按调用链可能在已持有 Store guard 的线程再次取非重入 Mutex；
    // 这是未经运行复现的源码风险，不能把此路径描述为已成功完成所有 payload 的核验。
    pub(super) fn verify_experiment_history(&self, connection: &Connection) -> StoreResult<()> {
        for artifact in read_kind_artifacts(connection, ArtifactKind::ExperimentTrial)? {
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "experiment trial must be canonical".to_owned(),
                ));
            }
            let trial: ExperimentTrial = self.read_artifact_payload(&artifact)?;
            trial.validate()?;
            if artifact.source_refs != trial.source_refs {
                return Err(StoreError::Integrity(
                    "experiment trial source closure mismatch".to_owned(),
                ));
            }
        }
        for artifact in read_kind_artifacts(connection, ArtifactKind::SearchBiasCertificate)? {
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "search bias certificate must be canonical".to_owned(),
                ));
            }
            let certificate: SearchBiasCertificate = self.read_artifact_payload(&artifact)?;
            certificate.validate()?;
            if artifact.source_refs != certificate.trial_refs {
                return Err(StoreError::Integrity(
                    "search bias certificate source closure mismatch".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
