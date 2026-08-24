import Foundation
import ObservatoryKit

// The mapping rules that must never regress, straight from the domain:
//  · optional step + Skipped        → Not Triggered (not success, not failure)
//  · non-Paper run + Paper Commit   → Not Applicable
//  · nil ppm metric                 → Unavailable (never 0)
//  · unsealed outcome               → may not read as Completed
//  · CompletedWithExecutionRejection→ completed *and* rejected
func runPresentationChecks() {
    Check.suite("Task status mapping") {
        Check.expect(
            TaskStatus.skipped.status(optional: true) == .notTriggered,
            "skipped optional step must read as Not Triggered"
        )
        Check.expect(
            TaskStatus.skipped.status(optional: false) == .skipped,
            "skipped mandatory step must read as Skipped, not Not Triggered"
        )
        Check.expect(
            TaskStatus.succeeded.status(optional: false, applicable: false) == .notApplicable,
            "an inapplicable step must read as Not Applicable regardless of task status"
        )
        Check.expect(TaskStatus.leased.status(optional: false) == .leased, "leased must stay distinct from pending")
        Check.expect(TaskStatus.pending.status(optional: false) == .queued, "pending maps to Queued")
        Check.expect(TaskStatus.succeeded.isTerminal, "succeeded is terminal")
        Check.expect(!TaskStatus.leased.isTerminal, "leased is not terminal")
    }

    Check.suite("Run purpose gating") {
        Check.expect(RunPurpose.paper.submitsPaperOrders, "canonical Paper submits orders")
        for purpose in [RunPurpose.debug, .paperDryRun, .replay, .shadow] {
            Check.expect(
                !purpose.submitsPaperOrders,
                "\(purpose.rawValue) must render Paper Commit as Not Applicable"
            )
        }
    Check.expect(!RunPurpose.debug.isCanonical, "debug is noncanonical")
    Check.expect(!RunPurpose.paperDryRun.isCanonical, "dry run is noncanonical")
    Check.expect(!RunPurpose.replay.isCanonical, "replay is noncanonical")
    Check.expect(!RunPurpose.shadow.isCanonical, "shadow is comparison-only, not canonical learning")
    Check.equal(RunPurpose.paperDryRun.rawValue, "paper_dry_run", "serde wire name")
        Check.equal(RunPurpose.allCases.count, 5, "RunPurpose variant count")
    }

    Check.suite("Domain enum parity") {
        Check.equal(WorkflowStatus.allCases.count, 8, "WorkflowStatus variant count")
        Check.equal(TaskStatus.allCases.count, 7, "TaskStatus variant count")
        Check.equal(HardBlocker.allCases.count, 21, "HardBlocker variant count")
        Check.equal(SoftWarning.allCases.count, 5, "SoftWarning variant count")
        Check.equal(OrderReceiptState.allCases.count, 6, "OrderReceiptState variant count")
        Check.equal(ReconciliationState.allCases.count, 4, "ReconciliationState variant count")
        Check.equal(OutcomeHorizonKind.allCases.count, 3, "OutcomeHorizon variant count")
        Check.equal(MemoryLifecycle.allCases.count, 5, "MemoryLifecycle variant count")
        Check.equal(CandidatePolicyState.allCases.count, 5, "CandidatePolicyState variant count")
        Check.equal(RetrospectiveCategory.allCases.count, 7, "RetrospectiveCategory variant count")
        Check.equal(RetrospectiveConclusion.allCases.count, 4, "RetrospectiveConclusion variant count")
        Check.equal(RetrospectiveStatus.allCases.count, 2, "RetrospectiveStatus variant count")
        Check.equal(TradableAsset.allCases.count, 4, "Asset variant count")

        Check.equal(HardBlocker.materialConflict.rawValue, "material_conflict", "blocker wire name")
        Check.equal(HardBlocker.materialConflict.gate, .decision, "material conflict belongs to the decision gate")
        Check.equal(HardBlocker.staleQuote.gate, .evidence, "stale quote belongs to the evidence gate")
        Check.equal(HardBlocker.turnoverLimit.gate, .execution, "turnover limit belongs to the execution gate")
        Check.equal(TradableAsset.tqqq.rawValue, "TQQQ", "assets use SCREAMING_SNAKE_CASE")
        Check.equal(OrderReceiptState.partiallyFilled.rawValue, "partially_filled", "receipt wire name")

        // The reference image shows "Pending / Working"; those are not real states.
        let receiptLabels = OrderReceiptState.allCases.map(\.displayName)
        Check.expect(!receiptLabels.contains("Working"), "\"Working\" is not a domain order state")
    }

    Check.suite("Workflow status semantics") {
        Check.equal(
            WorkflowStatus.completedWithExecutionRejection.status,
            .completedWithRejection,
            "rejection must carry both completion and rejection"
        )
        Check.expect(
            WorkflowStatus.completedWithExecutionRejection.displayName.contains("Rejected"),
            "rejection must be visible in the label"
        )
        Check.equal(WorkflowStatus.decisionCompleted.rawValue, "decision_completed", "wire name")
        Check.equal(WorkflowStatus.completed.status, .completed, "completed maps cleanly")
    }

    Check.suite("Outcome consistency") {
        let sealed = HorizonPresentation(
            horizon: .t1,
            status: .completed,
            progress: 1,
            evidenceCompletenessPpm: 1_000_000,
            isSealed: true,
            note: "Evidence complete"
        )
        let unsealed = HorizonPresentation(
            horizon: .t3,
            status: .completed,
            progress: 1,
            evidenceCompletenessPpm: 680_000,
            isSealed: false,
            note: "Evidence in progress"
        )
        let observing = HorizonPresentation(
            horizon: .t3,
            status: .observing,
            progress: 0.68,
            evidenceCompletenessPpm: 680_000,
            isSealed: false,
            note: "Evidence in progress"
        )
        Check.expect(sealed.isConsistent, "a sealed outcome may read Completed")
        Check.expect(!unsealed.isConsistent, "an unsealed outcome must not read Completed")
        Check.expect(observing.isConsistent, "observing is always consistent")

        let waiting = HorizonPresentation(
            horizon: .t5,
            status: .waiting,
            progress: nil,
            evidenceCompletenessPpm: nil,
            isSealed: false,
            note: "Window not reached"
        )
        Check.expect(waiting.progress == nil, "a waiting ring has no progress, not 0%")
        Check.equal(
            PpmFormatter.share(ppm: waiting.evidenceCompletenessPpm),
            "Unavailable",
            "missing completeness must not render as 0%"
        )

        let window = OutcomeWindowPresentation(
            horizon: .t1,
            portfolioReturnPpm: 14_600,
            benchmarkReturnPpm: 5_800,
            transactionCostPpm: 2_300,
            slippagePpm: 1_700,
            utilityPpm: 720_000,
            calibrationPpm: nil,
            evidenceCompletenessPpm: 680_000,
            riskRecallPpm: nil,
            winRatePpm: 620_000,
            profitFactorPpm: 1_480_000,
            sharpePpm: 1_350_000,
            maxDrawdownPpm: -23_100,
            comparison: []
        )
        Check.equal(window.alphaPpm, 8_800, "alpha is portfolio minus benchmark")
        Check.equal(window.netReturnPpm, 10_600, "net return subtracts cost and slippage")
        Check.equal(PpmFormatter.percent(ppm: window.alphaPpm), "+0.88%", "alpha formatting")
        Check.equal(
            PpmFormatter.ratio(ppm: window.calibrationPpm),
            "Unavailable",
            "nil calibration must stay Unavailable"
        )
        Check.equal(
            PpmFormatter.share(ppm: window.riskRecallPpm),
            "Unavailable",
            "nil risk recall must stay Unavailable"
        )
        Check.expect(PpmFormatter.fraction(ppm: window.riskRecallPpm) == nil, "nil risk recall draws no bar")
    }

    Check.suite("Live equity time axis") {
        let timestamp = ISO8601DateFormatter().date(from: "2026-08-17T13:30:00Z")!
        let point = EquityPoint(
            index: 0,
            minutesFromOpen: 0,
            timestamp: timestamp,
            portfolio: 100_000,
            benchmark: nil
        )
        Check.expect(point.chartX != Double(point.index), "live history uses its real timestamp for chart X")
        Check.expect(
            point.axisLabel(for: .oneDay, locale: Locale(identifier: "en_US_POSIX")).contains(":"),
            "1D labels render exchange-local time"
        )
        Check.expect(
            point.axisLabel(for: .oneMonth, locale: Locale(identifier: "en_US_POSIX")) != point.timeLabel,
            "multi-day labels render dates instead of fake market-open minutes"
        )
    }

    Check.suite("Workflow node mapping") {
        let critic = WorkflowNodePresentation(
            stage: .critic,
            taskStatus: .skipped,
            column: 3,
            row: 0
        )
        Check.equal(critic.status, .notTriggered, "an unfired Critic is Not Triggered")
        Check.equal(
            critic.status.detail,
            "No material conflict detected",
            "Not Triggered must explain itself"
        )

        let paperCommit = WorkflowNodePresentation(
            stage: .paperCommit,
            taskStatus: .succeeded,
            isApplicable: false,
            column: 6,
            row: 1
        )
        Check.equal(paperCommit.status, .notApplicable, "non-Paper Paper Commit is Not Applicable")
        Check.equal(
            paperCommit.status.detail,
            "This run does not submit Paper orders",
            "Not Applicable must explain itself"
        )

        let blocked = WorkflowNodePresentation(
            stage: .decisionGate,
            taskStatus: .failed,
            blockers: [.materialConflict, .noExecutableOrder],
            column: 5,
            row: 1
        )
        Check.expect(blocked.isBlocked, "a gate with blockers is blocked")
        Check.equal(blocked.blockers.count, 2, "blockers are preserved for the reject panel")

        Check.expect(WorkflowStageKind.critic.isOptional, "Critic is the only optional stage")
        Check.expect(!WorkflowStageKind.synthesizer.isOptional, "Synthesizer is mandatory")
        Check.expect(WorkflowStageKind.paperCommit.requiresPaperRun, "Paper Commit requires a Paper run")
        Check.equal(WorkflowStageKind.horizon(.t5).displayName, "T+5", "horizon naming")
        Check.equal(
            OutcomeHorizonKind.t5.windowLabel,
            "5 Trading Sessions",
            "horizons are counted in trading sessions, not calendar days"
        )
    }

    Check.suite("Execution verdict") {
        Check.equal(ExecutionVerdictKind.noOrder.displayName, "No Order", "no-order label")
        Check.expect(
            ExecutionVerdictKind.noOrder.status != .failed,
            "No Order must not be presented as a failure"
        )
        Check.equal(ExecutionVerdictKind.noOrder.rawValue, "no_order", "verdict wire name")
    }

    Check.suite("Reasoning intensity") {
        Check.equal(ReasoningIntensity.max.orbitCount, 4, "Max adds a fourth orbit ring")
        Check.expect(ReasoningIntensity.max.usesCoralAccent, "Max leans on coral, never purple")
        Check.equal(
            ReasoningIntensity.allCases.map(\.rawValue),
            ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
            "Core Settings exposes every supported reasoning effort"
        )
        Check.equal(ReasoningIntensity.none.orbitCount, 0, "None adds no synthetic orbit")
        Check.equal(ReasoningIntensity.xhigh.displayName, "XHigh", "xhigh label")
    }

    Check.suite("Core model routing") {
        let draft = CoreConfigurationDraft()
        Check.equal(draft.globalModel, "gpt-5.6-luna", "default model remains current")
        Check.equal(draft.globalReasoningEffort, "low", "default reasoning remains current")
        Check.equal(draft.globalResponseLanguage, "简体中文", "default language is Chinese")
        Check.equal(
            draft.stageModels.count,
            CoreModelStage.allCases.count,
            "every model-mediated Rust stage is configurable"
        )
        for stage in CoreModelStage.allCases {
            Check.equal(draft.stageModels[stage]?.model, "gpt-5.6-luna", "\(stage.id) model")
            Check.equal(draft.stageModels[stage]?.reasoningEffort, "low", "\(stage.id) reasoning")
            Check.equal(draft.stageModels[stage]?.responseLanguage, nil, "\(stage.id) inherits language")
        }
        let languageRoute = CoreStageModelConfiguration(
            model: "gpt-5.6-luna",
            reasoningEffort: "high",
            responseLanguage: "简体中文"
        )
        let encodedRoute = try? JSONEncoder().encode(languageRoute)
        let decodedRoute = encodedRoute.flatMap {
            try? JSONDecoder().decode(CoreStageModelConfiguration.self, from: $0)
        }
        Check.equal(decodedRoute?.responseLanguage, "简体中文", "stage language survives route JSON")
        Check.equal(
            RustCoreState.failed("observer decode failed").detail,
            "observer decode failed",
            "Core failure detail remains visible in Settings"
        )
        Check.expect(
            CoreCredentialStatus(
                llmAPIKey: true,
                alpacaAPIKey: true,
                alpacaAPISecret: true,
                fredAPIKey: false
            ).requiredComplete,
            "required credentials persist independently of optional FRED"
        )
        Check.expect(
            !CoreCredentialStatus(
                llmAPIKey: true,
                alpacaAPIKey: true,
                alpacaAPISecret: false,
                fredAPIKey: true
            ).requiredComplete,
            "missing Alpaca secret keeps Core fail-closed"
        )
    }

    Check.suite("Workflow node trace presentation") {
        let tool = StageToolEventPresentation(
            cursor: 7,
            callID: "call-7",
            name: "read_context",
            lifecycle: "completed",
            turn: 2
        )
        let output = StageLLMOutputPresentation(
            id: "sha256:claim",
            kind: "decision_proposal",
            createdAt: Date(timeIntervalSince1970: 0),
            body: "{\n  \"decision\": \"hold\"\n}",
            sequence: 8
        )
        let memo = AnalysisRecordPresentation(
            id: "memo-6",
            sequence: 6,
            kind: .researchMemo,
            actor: "Critic",
            title: "Research memo",
            body: "Natural-language evidence review."
        )
        let inspector = StageInspectorPresentation(
            stageTitle: "Critic",
            status: .completed,
            model: "gpt-5.6-luna",
            provider: "responses",
            reasoningMode: "high",
            turn: 2,
            totalTurns: 2,
            toolCalls: 1,
            latencyMillis: 320,
            inputTokens: 1_024,
            outputTokens: 256,
            confidencePpm: 800_000,
            summary: "Reviewed",
            conclusion: "Evidence supports a hold conclusion.",
            alternatives: [],
            uncertainties: [],
            toolEvents: [tool],
            llmOutputs: [output],
            transientAnalysisRecords: [memo]
        )
        let workflow = WorkflowPresentation(
            nodes: [],
            edges: [],
            activeStageID: nil,
            inspector: inspector,
            observedTradingDays: 0,
            totalTradingDays: 5,
            stageInspectors: ["critic": inspector]
        )
        let selected = workflow.inspector(for: "critic")
        Check.equal(selected.provider, "responses", "provider proves model-backed node")
        Check.equal(selected.inputTokens, 1_024, "input tokens stay visible")
        Check.equal(selected.outputTokens, 256, "output tokens stay visible")
        Check.equal(
            selected.conclusion,
            "Evidence supports a hold conclusion.",
            "validated model conclusion stays visible"
        )
        Check.equal(selected.toolEvents.first?.name, "read_context", "tool lifecycle stays on node")
        Check.equal(selected.llmOutputs.first?.displayKind, "Decision Proposal", "output kind label")
        Check.expect(
            selected.llmOutputs.first?.body.contains("hold") == true,
            "structured LLM payload stays available for inspector"
        )
        Check.equal(
            selected.analysisRecords.map(\.kind),
            [.researchMemo, .tool, .rustOutput],
            "chat transcript orders the LLM memo, tool lifecycle, then Rust-validated output"
        )
        Check.equal(
            selected.analysisRecords.first?.body,
            "Natural-language evidence review.",
            "draft memo remains the natural-language LLM record"
        )
        Check.equal(
            selected.analysisRecords.last?.kind.displayName,
            "Rust",
            "validated structured result is attributed to Rust"
        )
        Check.equal(
            selected.analysisRecords.last?.actor,
            "Rust",
            "validated result ownership stays with the Rust runtime"
        )
        Check.equal(
            selected.analysisRecords.last?.body,
            "Evidence supports a hold conclusion.",
            "Rust result renders the validated artifact projection"
        )
        Check.equal(
            AnalysisRecordKind.researchMemo.displayName,
            "LLM",
            "natural-language research memo is attributed to the LLM"
        )
        Check.expect(
            selected.analysisRecords.allSatisfy {
                let body = $0.body.lowercased()
                return !body.contains("tool_arguments") && !body.contains("submit_result")
            },
            "node record never invents or exposes terminal tool arguments"
        )
        let noToolInspector = StageInspectorPresentation(
            stageTitle: "Synthesizer",
            status: .completed,
            model: "gpt-5.6-luna",
            provider: "responses",
            reasoningMode: "low",
            turn: 1,
            totalTurns: 1,
            toolCalls: 0,
            latencyMillis: 100,
            inputTokens: 100,
            outputTokens: 50,
            confidencePpm: 700_000,
            summary: "Returned directly",
            conclusion: "Hold.",
            alternatives: [],
            uncertainties: [],
            toolEvents: [],
            llmOutputs: [output]
        )
        Check.expect(
            !noToolInspector.analysisRecords.contains { $0.kind == .tool },
            "zero tool calls produce no synthetic transcript row"
        )

        let mockCouncil = ScenarioLibrary.snapshot(.paperRunningSynthesizerActive).council
        Check.expect(mockCouncil.topics.isEmpty, "Intelligence topics are not synthesized from Mock council cards")
        Check.expect(
            mockCouncil.analysisRecords.isEmpty,
            "Intelligence analysis records require Observer trajectory or artifacts"
        )
    }
}
