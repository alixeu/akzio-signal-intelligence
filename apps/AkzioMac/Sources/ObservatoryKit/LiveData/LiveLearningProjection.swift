import Foundation

extension LiveProjection {
    static func learning(_ payload: ObserverSnapshotPayload) -> LearningPresentation {
        let section = payload.learning
        let artifacts = section.data?.artifacts ?? []
        let outcomeUtilityByArtifactID = Dictionary(uniqueKeysWithValues: artifacts
            .filter { $0.kind == "outcome" }
            .map { outcome in
                let utilities = outcome.payload["windows"]?.array?.compactMap {
                    $0["utility_ppm"]?.int
                } ?? []
                let average = utilities.isEmpty ? nil : utilities.reduce(0, +) / utilities.count
                return (outcome.artifactID, average)
            })
        let retrospectives = artifacts.filter { $0.kind == "retrospective" }
        let cards = retrospectives.map { artifact in
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
        let summary = section.data?.summary
        let transitions = section.data?.policyTransitions ?? []
        let metricTracks = (section.data?.policyMetrics ?? []).compactMap(livePolicyTrack)
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
            availabilityReason: section.reason
        )
    }

    static var unavailablePortfolio: PortfolioPresentation {
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
            observedTradingDays: 0,
            totalTradingDays: 5,
            outcomeID: MissingValue.unavailable.rawValue,
            availabilityStatus: .unavailable,
            availabilityReason: "No canonical Outcome is available yet"
        )
    }

    static var unavailableRun: RunPresentation {
        RunPresentation(
            runId: MissingValue.unavailable.rawValue,
            purpose: .paper,
            status: .queued,
            topology: MissingValue.unavailable.rawValue,
            model: MissingValue.unavailable.rawValue,
            market: "US Equities",
            startedAt: Date(timeIntervalSince1970: 0),
            elapsedSeconds: 0,
            systemHealthPpm: 0,
            marketOpen: false,
            dataLive: false,
            dataStale: true,
            latencyMillis: 0,
            brokerSession: MissingValue.unavailable.rawValue
        )
    }

    static var unavailableArchive: ArchivePresentation {
        ArchivePresentation(
            rows: [],
            totalRuns: 0,
            successRatePpm: 0,
            page: 1,
            pageSize: 1,
            selectedRowID: nil,
            activeFilters: []
        )
    }

    static var unavailableCouncil: CouncilPresentation {
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
