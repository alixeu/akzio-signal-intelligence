import Foundation

extension LiveProjection {
    static func events(_ payload: ObserverSnapshotPayload) -> [EventPresentation] {
        let events = payload.currentRun?.events ?? []
        if events.isEmpty {
            return [
                EventPresentation(
                    id: "observer-\(payload.eventCursor)",
                    title: "Observer synchronized",
                    detail: "No durable run event is available yet.",
                    severity: .info,
                    symbol: "arrow.triangle.2.circlepath",
                    timestamp: payload.generatedAt,
                    relativeLabel: "Live"
                )
            ]
        }
        return events.suffix(12).map { event in
            EventPresentation(
                id: "event-\(event.cursor)",
                title: event.eventType
                    .replacingOccurrences(of: "_", with: " ")
                    .replacingOccurrences(of: ".", with: " ")
                    .capitalized,
                detail: event.taskID.map { "Task \($0)" } ?? "Run lifecycle",
                severity: event.eventType.contains("failed") ? .critical : .info,
                symbol: event.eventType.contains("failed")
                    ? "exclamationmark.triangle"
                    : "point.3.connected.trianglepath.dotted",
                timestamp: event.createdAt,
                relativeLabel: liveTimeLabel(event.createdAt)
            )
        }
    }

    static func council(
        _ payload: ObserverSnapshotPayload,
        reasoningRecords: [LiveReasoningRecord]
    ) -> CouncilPresentation {
        let detail = payload.currentRun
        let tasks = detail?.workflow.tasks ?? []
        let trajectory = detail?.trajectory ?? []
        var roles: [RoleCardPresentation] = []
        for task in tasks {
            let role = liveRole(task.node.recipeID)
            guard !roles.contains(where: { $0.role == role }) else { continue }
            let entries = trajectory.filter { $0.taskID == task.node.taskID }
            let model = entries.compactMap(\.model?.modelID).last ?? MissingValue.unavailable.rawValue
            let reasoning = entries.compactMap(\.model?.reasoningEffort).last
            let deliberation = entries.compactMap(\.deliberation).last
            roles.append(
                RoleCardPresentation(
                    role: role,
                    model: model,
                    status: liveTaskStatus(task.taskStatus),
                    tokensIn: entries.compactMap(\.inputTokens).last.map(Int.init),
                    tokensOut: entries.compactMap(\.outputTokens).last.map(Int.init),
                    toolCalls: entries.filter { $0.eventType.contains("tool") }.count,
                    latencyMillis: entries.compactMap(\.latencyMillis).last.map(Int.init),
                    confidencePpm: deliberation.map { Int($0.confidencePpm) },
                    intensity: ReasoningIntensity(rawValue: reasoning ?? "") ?? .medium
                )
            )
        }
        let selected = roles.first(where: { $0.role == .synthesizer })?.role
            ?? roles.first?.role
            ?? .synthesizer
        let selectedTaskIDs = tasks
            .filter { liveRole($0.node.recipeID) == selected }
            .map(\.node.taskID)
        let selectedEntries = trajectory.filter { entry in
            entry.taskID.map(selectedTaskIDs.contains) ?? false
        }
        let model = selectedEntries.compactMap(\.model?.modelID).last
            ?? roles.first(where: { $0.role == selected })?.model
            ?? MissingValue.unavailable.rawValue
        let reasoning = selectedEntries.compactMap(\.model?.reasoningEffort).last
        let deliberation = selectedEntries.compactMap(\.deliberation).last
        let alternativeScores = deliberation?.alternativeMatchPpm ?? []
        let alternatives = (deliberation?.alternatives ?? []).enumerated().map { index, value in
            AlternativePresentation(
                tag: "A\(index + 1)",
                label: value,
                matchPpm: alternativeScores.indices.contains(index)
                    ? Int(alternativeScores[index])
                    : nil
            )
        }
        let uncertaintyValues = deliberation?.uncertainties ?? []
        let uncertaintyWeights = deliberation?.uncertaintyWeightPpm ?? []
        let uncertainties = uncertaintyValues.enumerated().map { index, value in
            UncertaintyPresentation(
                label: value,
                weightPpm: uncertaintyWeights.indices.contains(index)
                    ? Int(uncertaintyWeights[index])
                    : nil
            )
        }
        let models = Array(Set(trajectory.compactMap(\.model?.modelID))).sorted()
        let roleByTaskID = Dictionary(uniqueKeysWithValues: tasks.map {
            ($0.node.taskID, liveRole($0.node.recipeID))
        })
        let artifactsByID = Dictionary(uniqueKeysWithValues: (detail?.artifacts ?? []).map {
            ($0.artifactID, $0)
        })
        let eventDatesByCursor = Dictionary(uniqueKeysWithValues: (detail?.events ?? []).map {
            ($0.cursor, $0.createdAt)
        })
        var seenTopics = Set<String>()
        var seenArtifacts = Set<String>()
        var topics: [IntelligenceTopicPresentation] = []
        var analysisRecords: [AnalysisRecordPresentation] = []

        func addTopic(_ title: String, kind: IntelligenceTopicKind, source: String) {
            let value = title.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !value.isEmpty else { return }
            let key = "\(kind.rawValue):\(value.lowercased())"
            guard seenTopics.insert(key).inserted else { return }
            topics.append(
                IntelligenceTopicPresentation(
                    id: key,
                    kind: kind,
                    title: value,
                    source: source
                )
            )
        }

        for entry in trajectory.sorted(by: { $0.cursor < $1.cursor }) {
            let actor = entry.taskID
                .flatMap { roleByTaskID[$0] }
                .map(\.displayName)
                ?? "Run"
            if let deliberation = entry.deliberation {
                addTopic(deliberation.selectedPath, kind: .topic, source: actor)
                deliberation.uncertainties.forEach {
                    addTopic($0, kind: .issue, source: actor)
                }
                deliberation.alternatives.forEach {
                    addTopic($0, kind: .alternative, source: actor)
                }
                analysisRecords.append(
                    AnalysisRecordPresentation(
                        id: "analysis-\(entry.cursor)",
                        sequence: entry.cursor,
                        kind: .analysis,
                    actor: actor,
                    title: "Deliberation",
                    body: deliberation.selectedPath,
                    createdAt: eventDatesByCursor[entry.cursor],
                    model: entry.model?.modelID,
                        reasoningMode: entry.model?.reasoningEffort,
                        latencyMillis: entry.latencyMillis.map(Int.init),
                        inputTokens: entry.inputTokens.map(Int.init),
                        outputTokens: entry.outputTokens.map(Int.init)
                    )
                )
            }
            if let tool = entry.tool {
                let detail = [
                    tool.lifecycle.capitalized,
                    entry.turn.map { "T\($0)" },
                    tool.callID,
                ].compactMap { $0 }.joined(separator: " · ")
                analysisRecords.append(
                    AnalysisRecordPresentation(
                        id: "tool-\(entry.cursor)",
                        sequence: entry.cursor,
                        kind: .tool,
                    actor: actor,
                    title: tool.name ?? MissingValue.unavailable.rawValue,
                    body: detail,
                    createdAt: eventDatesByCursor[entry.cursor]
                )
                )
            }

            let artifactIDs = ([entry.artifactID].compactMap { $0 }
                + (entry.outputRefs ?? []).map(\.artifactID))
            for artifactID in artifactIDs where seenArtifacts.insert(artifactID).inserted {
                guard let artifact = artifactsByID[artifactID],
                      let body = liveAnalysisText(artifact)
                else { continue }
                let topicKind = liveTopicKind(artifact.kind)
                addTopic(body, kind: topicKind, source: actor)
                analysisRecords.append(
                    AnalysisRecordPresentation(
                        id: "artifact-\(artifact.artifactID)",
                        sequence: entry.cursor,
                        kind: topicKind == .conclusion ? .conclusion : .llmOutput,
                        actor: actor,
                        title: artifact.kind.replacingOccurrences(of: "_", with: " ").capitalized,
                        body: body,
                        createdAt: artifact.createdAt,
                        model: entry.model?.modelID,
                        reasoningMode: entry.model?.reasoningEffort,
                        latencyMillis: entry.latencyMillis.map(Int.init),
                        inputTokens: entry.inputTokens.map(Int.init),
                        outputTokens: entry.outputTokens.map(Int.init)
                    )
                )
            }
        }
        analysisRecords.append(contentsOf: reasoningRecords.map(\.presentation))
        return CouncilPresentation(
            roles: roles,
            selectedRole: selected,
            selectedModelName: model,
            selectedModelSummary: deliberation == nil
                ? (selectedEntries.last?.eventType ?? "No model turn observed.")
                : (deliberation?.assessmentSource == "model_assessed"
                    ? "Model-assessed deliberation"
                    : "Deliberation scores unavailable"),
            selectedPath: deliberation.map { [$0.selectedPath] } ?? [],
            intensity: ReasoningIntensity(rawValue: reasoning ?? "") ?? .medium,
            gallery: models.map { ModelOption(name: $0, tier: "Observed", isSelected: $0 == model) },
            alternatives: alternatives,
            uncertainties: uncertainties,
            basisArtifacts: (deliberation?.basisArtifactIDs ?? []).map {
                BasisArtifact(label: String($0.prefix(12)), symbol: "doc.text.magnifyingglass")
            },
            overallUncertaintyPpm: deliberation.map { max(0, 1_000_000 - Int($0.confidencePpm)) },
            topics: topics,
            analysisRecords: analysisRecords.sorted {
                $0.sequence == $1.sequence ? $0.id < $1.id : $0.sequence < $1.sequence
            }
        )
    }

    static func portfolio(_ payload: ObserverSnapshotPayload) -> PortfolioPresentation {
        guard let portfolio = payload.portfolio.data else { return unavailablePortfolio }
        let currentArtifacts = payload.currentRun?.artifacts ?? []
        let plan = currentArtifacts.last(where: { $0.kind == "execution_plan" })
        let receipts = currentArtifacts.filter { $0.kind == "order_receipt" }
        let reconciliationArtifact = currentArtifacts.last(where: { $0.kind == "reconciliation" })
        let targetWeights = plan?.payload["target"]?["weights"]?.object ?? [:]
        let positions = portfolio.positions.compactMap { position -> PositionPresentation? in
            guard let asset = TradableAsset(rawValue: position.symbol.uppercased()) else { return nil }
            let actual = liveRatio(position.marketValueMicros, portfolio.equityMicros)
            let target = targetWeights[asset.rawValue.lowercased()]?.int ?? 0
            return PositionPresentation(
                asset: asset,
                weightPpm: actual,
                marketValueMicros: position.marketValueMicros,
                pnlMicros: position.unrealizedPnlMicros,
                pnlPpm: position.unrealizedPnlPpm.map(Int.init),
                spark: (position.sparklinePpm ?? []).map(Double.init),
                actualPpm: actual,
                targetPpm: target
            )
        }
        let allocations = positions.map {
            AllocationRow(label: $0.asset.rawValue, actualPpm: $0.actualPpm, targetPpm: $0.targetPpm)
        }
        let planOrders = plan?.payload["orders"]?.array ?? []
        let orders = receipts.compactMap { receipt -> OrderPresentation? in
            guard let assetName = receipt.payload["asset"]?.string?.uppercased(),
                  let asset = TradableAsset(rawValue: assetName)
            else { return nil }
            let planOrder = planOrders.first { $0["asset"]?.string?.uppercased() == assetName }
            let side = OrderSide(rawValue: planOrder?["side"]?.string ?? "") ?? .buy
            let state = OrderReceiptState(rawValue: receipt.payload["state"]?.string ?? "") ?? .failed
            return OrderPresentation(
                id: receipt.payload["client_order_id"]?.string ?? receipt.artifactID,
                timeLabel: liveTimeLabel(receipt.createdAt),
                asset: asset,
                side: side,
                type: "Limit",
                quantityMicros: receipt.payload["requested_quantity_micros"]?.int64 ?? 0,
                limitPriceMicros: planOrder?["limit_price"]?.int64,
                state: state
            )
        }
        let fills = (portfolio.fills?.data ?? []).compactMap { fill -> FillPresentation? in
            guard let asset = TradableAsset(rawValue: fill.symbol.uppercased()),
                  let side = OrderSide(rawValue: fill.side)
            else { return nil }
            return FillPresentation(
                id: fill.activityID,
                timeLabel: liveTimeLabel(fill.transactionAt),
                asset: asset,
                side: side,
                quantityMicros: fill.quantityMicros,
                priceMicros: fill.priceMicros,
                venue: fill.venue ?? MissingValue.unavailable.rawValue
            )
        }
        let verdictArtifact = currentArtifacts.last(where: { $0.kind == "execution_verdict" })
        let verdict: ExecutionVerdictKind = verdictArtifact?.payload.object?.keys.contains("accepted") == true
            ? .accepted
            : .noOrder
        let reconciliation = ReconciliationState(
            rawValue: reconciliationArtifact?.payload["state"]?.string ?? "pending"
        ) ?? .pending
        let flow = [
            AllocationFlowStage(
                title: "Decision",
                symbol: "checkmark.seal",
                isActive: currentArtifacts.contains { $0.kind == "decision" }
            ),
            AllocationFlowStage(title: "Plan", symbol: "list.bullet.rectangle", isActive: plan != nil),
            AllocationFlowStage(title: "Broker", symbol: "building.columns", isActive: !receipts.isEmpty),
            AllocationFlowStage(
                title: "Reconcile",
                symbol: "arrow.triangle.2.circlepath",
                isActive: reconciliationArtifact != nil
            ),
        ]
        let leverage = plan?.payload["factor_exposure"]?["leveraged_equity_ppm"]?.int
        let analytics = portfolio.analytics?.data
        return PortfolioPresentation(
            equityMicros: portfolio.equityMicros,
            todayPnlMicros: portfolio.dayPnlMicros ?? 0,
            todayPnlPpm: Int(portfolio.dayPnlPpm ?? 0),
            unrealizedPnlMicros: portfolio.positions.compactMap(\.unrealizedPnlMicros).reduce(0, +),
            realizedPnlMicros: portfolio.realizedPnlMicros,
            unrealizedPnlPpm: nil,
            realizedPnlPpm: portfolio.realizedPnlPpm.map(Int.init),
            curve: [],
            range: .oneDay,
            benchmarkLabel: analytics?.benchmarkSymbol ?? "QQQ",
            allocations: allocations,
            positions: positions,
            orders: orders,
            fills: fills,
            flow: flow,
            risk: RiskPresentation(
                betaPpm: analytics?.betaPpm.map(Int.init),
                volatilityPpm: analytics.map { Int($0.volatilityPpm) },
                maxDrawdownPpm: analytics.map { Int($0.maxDrawdownPpm) },
                varMicros: analytics?.var95Micros,
                leveragePpm: leverage,
                isElevated: payload.health.frozen
            ),
            verdict: verdict,
            reconciliation: reconciliation
        )
    }

    static func outcome(_ payload: ObserverSnapshotPayload) -> OutcomePresentation {
        let section = payload.outcome
        let analytics = section?.data
        let artifact = payload.learning.data?.artifacts.last(where: { $0.kind == "outcome" })
            ?? payload.currentRun?.artifacts.last(where: { $0.kind == "outcome" })
        let legacyWindows = artifact?.payload["windows"]?.array ?? []

        let windows = OutcomeHorizonKind.allCases.compactMap { horizon -> OutcomeWindowPresentation? in
            let metrics = analytics?.horizons.first { $0.horizon == horizon.rawValue }
            if let window = metrics?.window {
                return liveOutcomeWindow(window, metrics: metrics)
            }
            guard let value = legacyWindows.first(where: { $0["horizon"]?.string == horizon.rawValue }) else {
                return nil
            }
            return liveOutcomeWindow(value, metrics: metrics)
        }
        let availability = liveObserverSectionStatus(section?.status)
        let horizons = OutcomeHorizonKind.allCases.map { horizon in
            let metrics = analytics?.horizons.first { $0.horizon == horizon.rawValue }
            let window = windows.first { $0.horizon == horizon }
            let progress = metrics.map {
                Double($0.progressPpm) / PpmFormatter.ppmPerUnit
            }
            let status: AkzioStatus = if window != nil {
                .completed
            } else if (metrics?.progressPpm ?? 0) > 0 {
                .observing
            } else {
                .waiting
            }
            return HorizonPresentation(
                horizon: horizon,
                status: status,
                progress: progress,
                evidenceCompletenessPpm: window?.evidenceCompletenessPpm,
                isSealed: window != nil,
                note: window == nil
                    ? (section?.reason ?? "Awaiting sealed trading-session evidence")
                    : "Sealed from canonical Paper evidence"
            )
        }
        return OutcomePresentation(
            horizons: horizons,
            windows: windows,
            selected: windows.last?.horizon ?? .t1,
            observedTradingDays: Int(analytics?.completedTradingSessions ?? 0),
            totalTradingDays: OutcomeHorizonKind.t5.tradingDays,
            outcomeID: analytics?.outcomeID
                ?? artifact?.payload["outcome_id"]?.string
                ?? artifact?.artifactID
                ?? MissingValue.unavailable.rawValue,
            availabilityStatus: availability,
            availabilityReason: section?.reason
        )
    }

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

private func liveObserverSectionStatus(_ status: String?) -> AkzioStatus {
    switch status {
    case "available": .completed
    case "pending": .waiting
    case "stale": .stale
    default: .unavailable
    }
}

private func liveOutcomeWindow(
    _ value: ObserverOutcomeWindowPayload,
    metrics: ObserverOutcomeHorizonPayload?
) -> OutcomeWindowPresentation {
    OutcomeWindowPresentation(
        horizon: OutcomeHorizonKind(rawValue: value.horizon) ?? .t1,
        portfolioReturnPpm: Int(value.portfolioReturnPpm),
        benchmarkReturnPpm: Int(value.benchmarkReturnPpm),
        transactionCostPpm: Int(value.transactionCostPpm),
        slippagePpm: Int(value.slippagePpm),
        utilityPpm: Int(value.utilityPpm),
        calibrationPpm: value.calibrationPpm.map(Int.init),
        evidenceCompletenessPpm: Int(value.evidenceCompletenessPpm),
        riskRecallPpm: value.riskRecallPpm.map(Int.init),
        winRatePpm: metrics?.winRatePpm.map(Int.init),
        profitFactorPpm: metrics?.profitFactorPpm.map(Int.init),
        sharpePpm: metrics?.sharpePpm.map(Int.init),
        maxDrawdownPpm: metrics?.maxDrawdownPpm.map(Int.init),
        comparison: liveOutcomeComparison(metrics)
    )
}

private func liveOutcomeWindow(
    _ value: JSONValue,
    metrics: ObserverOutcomeHorizonPayload?
) -> OutcomeWindowPresentation? {
    guard let horizon = value["horizon"]?.string.flatMap(OutcomeHorizonKind.init(rawValue:)),
          let portfolioReturn = value["portfolio_return_ppm"]?.int,
          let benchmarkReturn = value["benchmark_return_ppm"]?.int,
          let transactionCost = value["transaction_cost_ppm"]?.int,
          let slippage = value["slippage_ppm"]?.int,
          let utility = value["utility_ppm"]?.int,
          let completeness = value["evidence_completeness_ppm"]?.int
    else { return nil }
    return OutcomeWindowPresentation(
        horizon: horizon,
        portfolioReturnPpm: portfolioReturn,
        benchmarkReturnPpm: benchmarkReturn,
        transactionCostPpm: transactionCost,
        slippagePpm: slippage,
        utilityPpm: utility,
        calibrationPpm: value["calibration_ppm"]?.int,
        evidenceCompletenessPpm: completeness,
        riskRecallPpm: value["risk_recall_ppm"]?.int,
        winRatePpm: metrics?.winRatePpm.map(Int.init),
        profitFactorPpm: metrics?.profitFactorPpm.map(Int.init),
        sharpePpm: metrics?.sharpePpm.map(Int.init),
        maxDrawdownPpm: metrics?.maxDrawdownPpm.map(Int.init),
        comparison: liveOutcomeComparison(metrics)
    )
}

private func liveOutcomeComparison(
    _ metrics: ObserverOutcomeHorizonPayload?
) -> [EquityPoint] {
    (metrics?.comparison ?? []).enumerated().map { index, point in
        EquityPoint(
            index: index,
            minutesFromOpen: index * 390,
            portfolio: Double(point.portfolioPpm) / 10_000,
            benchmark: Double(point.benchmarkPpm) / 10_000
        )
    }
}

private func livePolicyTrack(_ value: JSONValue) -> PolicyTrackPresentation? {
    guard let subjectValue = value["transition"]?["subject"],
          let subject = subjectValue["kind"]?.string.flatMap(PolicySubjectKind.init(rawValue:)),
          let name = subjectValue["id"]?.string
    else { return nil }
    let to = value["transition"]?["to"]
    let stateValue = to?["state"]?.string
    return PolicyTrackPresentation(
        subject: subject,
        name: String(name.prefix(24)),
        memoryState: subject == .memory ? stateValue.flatMap(MemoryLifecycle.init(rawValue:)) : nil,
        candidateState: subject == .memory
            ? nil
            : stateValue.flatMap(CandidatePolicyState.init(rawValue:)),
        activeSinceLabel: value["transition"]?["created_at"]?.string ?? MissingValue.unavailable.rawValue,
        sampleCount: 0,
        winRatePpm: nil,
        netImpactPpm: nil,
        stabilityPpm: nil,
        exposurePpm: nil
    )
}

private func livePolicyTrack(_ metric: ObserverPolicyMetricPayload) -> PolicyTrackPresentation? {
    guard let subject = metric.subject["kind"]?.string.flatMap(PolicySubjectKind.init(rawValue:)),
          let name = metric.subject["id"]?.string
    else { return nil }
    let stateValue = metric.state["state"]?.string
    return PolicyTrackPresentation(
        subject: subject,
        name: String(name.prefix(24)),
        memoryState: subject == .memory ? stateValue.flatMap(MemoryLifecycle.init(rawValue:)) : nil,
        candidateState: subject == .memory
            ? nil
            : stateValue.flatMap(CandidatePolicyState.init(rawValue:)),
        activeSinceLabel: "Latest durable evaluation",
        sampleCount: metric.sampleCount,
        winRatePpm: metric.winRatePpm.map(Int.init),
        netImpactPpm: metric.netImpactPpm.map(Int.init),
        stabilityPpm: metric.stabilityPpm.map(Int.init),
        exposurePpm: metric.exposurePpm.map(Int.init)
    )
}

private func liveTimelineKind(_ artifactKind: String) -> TimelineNodePresentation.Kind {
    switch artifactKind {
    case "outcome", "outcome_schedule": .outcome
    case "retrospective", "experience": .lesson
    case "evaluation": .decision
    default: .event
    }
}

private func liveAnalysisText(_ artifact: ObserverArtifactPayload) -> String? {
    let values: [String?]
    switch artifact.kind {
    case "workflow_proposal_draft":
        values = [artifact.payload["summary"]?.string, artifact.payload["objective"]?.string]
    case "claim":
        values = [artifact.payload["statement"]?.string]
    case "critique", "decision_proposal", "retrospective_draft":
        values = [artifact.payload["summary"]?.string, artifact.payload["conclusion"]?.string]
    default:
        return nil
    }
    return values.compactMap { $0 }
        .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        .first { !$0.isEmpty }
}

private func liveTopicKind(_ artifactKind: String) -> IntelligenceTopicKind {
    switch artifactKind {
    case "critique": .issue
    case "decision_proposal", "retrospective_draft": .conclusion
    default: .topic
    }
}

private func liveRole(_ recipeID: String) -> AgentRole {
    let value = recipeID.lowercased()
    if value.contains("planner") { return .planner }
    if value.contains("critic") { return .critic }
    if value.contains("synth") || value.contains("decision") { return .synthesizer }
    if value.contains("outcome") || value.contains("learning") { return .outcomeWorker }
    return .analyst
}

private func liveTaskStatus(_ rawValue: String) -> AkzioStatus {
    (TaskStatus(rawValue: rawValue) ?? .pending).status(optional: false)
}

private func liveRatio(_ value: Int64, _ total: Int64) -> Int {
    guard total != 0 else { return 0 }
    return Int((Double(value) * 1_000_000 / Double(total)).rounded())
}

private func liveTimeLabel(_ date: Date) -> String {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "HH:mm:ss"
    return formatter.string(from: date)
}

private func liveDateLabel(_ date: Date) -> String {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.dateFormat = "yyyy-MM-dd"
    return formatter.string(from: date)
}
