import Foundation

extension LiveProjection {
    static func learning(_ payload: ObserverSnapshotPayload) -> LearningPresentation {
        // Learning 页面只读取 Core 返回的 artifacts/summary/transitions；Swift 计算的 utility 平均值仅用于展示。
        let section = payload.learning
        let artifacts = section.data?.artifacts ?? []
        let outcomeUtilityByArtifactID = Dictionary(uniqueKeysWithValues: artifacts
            .filter { $0.kind == "outcome" }
            .map { outcome in
                // 窗口缺失或没有 utility 时保持 nil；平均值不是 Rust 的 canonical marginal utility。
                let utilities = outcome.payload["windows"]?.array?.compactMap {
                    $0["utility_ppm"]?.int
                } ?? []
                let average = utilities.isEmpty ? nil : utilities.reduce(0, +) / utilities.count
                return (outcome.artifactID, average)
            })
        let retrospectives = artifacts.filter { $0.kind == "retrospective" }
        let cards = retrospectives.map { artifact in
            // retrospective 的 findings/category/conclusion 都是可选 JSON 投影，未知值降为 unresolved/空集合。
            let findings = artifact.payload["findings"]?.array ?? []
            let conclusion = findings.compactMap { $0["conclusion"]?.string }
                .compactMap(RetrospectiveConclusion.init(rawValue:)).first ?? .unresolved
            let categories = findings.compactMap { $0["category"]?.string }
                .compactMap(RetrospectiveCategory.init(rawValue:))
            let outcomeArtifactID = artifact.payload["outcome"]?["artifact_id"]?.string
            let impactPpm = outcomeArtifactID.flatMap { outcomeUtilityByArtifactID[$0] } ?? nil
            return RetrospectiveCardPresentation(
                id: artifact.artifactID,
                title: artifact.payload["summary"]?.string ?? "Retrospective",
                dateLabel: liveDateLabel(artifact.createdAt),
                conclusion: conclusion,
                status: RetrospectiveStatus(
                    rawValue: artifact.payload["status"]?.string ?? "model_unavailable"
                ) ?? .modelUnavailable,
                categories: categories,
                pnlMicros: nil,
                impactPpm: impactPpm,
                spark: [],
                counterfactual: artifact.payload["counterfactuals"]?.array?.first?.string ?? "",
                lessonCandidate: artifact.payload["lesson_candidates"]?.array?.first?.string ?? "",
                diagnosticGaps: artifact.payload["diagnostic_gaps"]?.array?.compactMap(\.string) ?? [],
                tags: categories.map(\.rawValue),
                impact: conclusion == .failed ? .critical : .info
            )
        }
        let timeline = artifacts.enumerated().map { index, artifact in
            TimelineNodePresentation(
                id: artifact.artifactID,
                kind: liveTimelineKind(artifact.kind),
                label: artifact.kind.replacingOccurrences(of: "_", with: " ").capitalized,
                dateLabel: liveDateLabel(artifact.createdAt),
                detail: artifact.payload["summary"]?.string ?? artifact.kind,
                position: artifacts.count <= 1 ? 0 : Double(index) / Double(artifacts.count - 1),
                isCurrent: index == artifacts.count - 1
            )
        }
        // summary、transitions 和 researchAudit 都可缺失；Optional 链只填充观察到的字段，其余交给可用性状态解释。
        let summary = section.data?.summary
        let transitions = section.data?.policyTransitions ?? []
        // 有 metric tracks 时优先使用带样本统计的投影，否则退回 transition 轨迹；两者都为空仍显示 unavailable。
        let metricTracks = (section.data?.policyMetrics ?? []).compactMap(livePolicyTrack)
        // 返回值是不可变的 UI projection；它不改变 Core artifact，也不把 research audit 提升为 Outcome/Learning 完成。
        return LearningPresentation(
            cards: cards,
            timeline: timeline,
            policyTracks: metricTracks.isEmpty
                ? transitions.compactMap(livePolicyTrack)
                : metricTracks,
            impact: ImpactSummaryPresentation(
                totalImpactMicros: summary?.attributedUtilityMicros,
                totalImpactPpm: Int(summary?.attributedUtilityPpm ?? 0),
                lessonsCreated: summary?.lessonCandidates ?? 0,
                lessonsDelta: Int(summary?.lessonCandidatesDelta ?? 0),
                policiesEvolved: summary?.policiesEvolved ?? 0,
                policiesDelta: Int(summary?.policiesEvolvedDelta ?? 0),
                areas: (summary?.impactAreas ?? []).map {
                    ImpactAreaPresentation(
                        label: $0.category.replacingOccurrences(of: "_", with: " ").capitalized,
                        impactPpm: Int($0.impactPpm)
                    )
                }
            ),
            activePolicyName: metricTracks.last?.name
                ?? MissingValue.unavailable.rawValue,
            timeRangeLabel: summary.map {
                "Last \($0.rangeDays) days vs previous 30"
            } ?? (section.reason ?? "No canonical learning available"),
            availabilityStatus: liveObserverSectionStatus(section.status),
            availabilityReason: section.reason,
            researchAudit: payload.currentRun?.researchAudit?.records.filter {
                $0.producer == "learning.retrieval.audit" || $0.producer == "learning.revalidation.suggestion"
            }.flatMap(\.rows) ?? []
        )
    }

    static var unavailablePortfolio: PortfolioPresentation {
        // unavailable 是显式占位值：0/空集合表示“没有可用 live projection”，不代表真实账户为零。
        PortfolioPresentation(
            equityMicros: 0,
            todayPnlMicros: 0,
            todayPnlPpm: 0,
            unrealizedPnlMicros: nil,
            realizedPnlMicros: nil,
            unrealizedPnlPpm: nil,
            realizedPnlPpm: nil,
            curve: [],
            range: .oneDay,
            benchmarkLabel: MissingValue.unavailable.rawValue,
            allocations: [],
            positions: [],
            orders: [],
            fills: [],
            flow: [],
            risk: RiskPresentation(
                betaPpm: nil,
                volatilityPpm: nil,
                maxDrawdownPpm: nil,
                varMicros: nil,
                leveragePpm: nil,
                isElevated: false
            ),
            verdict: .noOrder,
            reconciliation: .pending
        )
    }

    static var unavailableOutcome: OutcomePresentation {
        // Outcome 缺失时所有 horizon 保持 waiting 且未 sealed，UI 不把自然时间流逝当成窗口完成。
        OutcomePresentation(
            horizons: OutcomeHorizonKind.allCases.map {
                HorizonPresentation(
                    horizon: $0,
                    status: .waiting,
                    progress: nil,
                    evidenceCompletenessPpm: nil,
                    isSealed: false,
                    note: "No canonical outcome available"
                )
            },
            windows: [],
            selected: .t1,
            observedTradingDays: nil,
            totalTradingDays: 5,
            outcomeID: MissingValue.unavailable.rawValue,
            availabilityStatus: .unavailable,
            availabilityReason: "No canonical Outcome is available yet"
        )
    }

    static var unavailableRun: RunPresentation {
        // 无当前 Run 时用明确的 stale/unavailable 组合占位，避免时间戳或市场状态被误读为 live 事实。
        RunPresentation(
            runId: MissingValue.unavailable.rawValue,
            purpose: .paper,
            status: .queued,
            topology: MissingValue.unavailable.rawValue,
            model: MissingValue.unavailable.rawValue,
            market: "US Equities",
            startedAt: Date(timeIntervalSince1970: 0),
            elapsedSeconds: 0,
            systemHealthPpm: nil,
            marketOpen: false,
            marketStatusKnown: false,
            dataLive: false,
            dataStale: true,
            latencyMillis: nil,
            brokerSession: MissingValue.unavailable.rawValue
        )
    }

    static var unavailableArchive: ArchivePresentation {
        // archive 缺失时保持空 rows 和 nil success rate，不能从当前 Run 推导历史成功率。
        ArchivePresentation(
            rows: [],
            totalRuns: 0,
            successRatePpm: nil,
            page: 1,
            pageSize: 1,
            selectedRowID: nil,
            activeFilters: []
        )
    }

    static var unavailableCouncil: CouncilPresentation {
        // council 缺失时不创建 model/trajectory；默认角色只为 UI 结构完整，不代表模型已运行。
        CouncilPresentation(
            roles: [],
            selectedRole: .synthesizer,
            selectedModelName: MissingValue.unavailable.rawValue,
            selectedModelSummary: "No durable model trajectory is available.",
            selectedPath: [],
            intensity: .medium,
            gallery: [],
            alternatives: [],
            uncertainties: [],
            basisArtifacts: [],
            overallUncertaintyPpm: nil
        )
    }

    static var unavailableLearning: LearningPresentation {
        // 没有 canonical learning 时使用不可用状态，而不是从 debug/isolated artifacts 推断学习成功。
        LearningPresentation(
            cards: [],
            timeline: [],
            policyTracks: [],
            impact: ImpactSummaryPresentation(
                totalImpactMicros: nil,
                totalImpactPpm: 0,
                lessonsCreated: 0,
                lessonsDelta: 0,
                policiesEvolved: 0,
                policiesDelta: 0,
                areas: []
            ),
            activePolicyName: MissingValue.unavailable.rawValue,
            timeRangeLabel: "No canonical learning available",
            availabilityStatus: .unavailable,
            availabilityReason: "No canonical learning artifacts are available yet"
        )
    }

    static var unavailableHealth: [HealthMetric] {
        // health 的空投影把 Core 标记为 elevated risk，提醒 UI 不能把缺失状态当作健康。
        [
            HealthMetric(
                id: "core",
                label: "Rust Core",
                value: MissingValue.unavailable.rawValue,
                fraction: nil,
                isElevatedRisk: true
            )
        ]
    }
}
