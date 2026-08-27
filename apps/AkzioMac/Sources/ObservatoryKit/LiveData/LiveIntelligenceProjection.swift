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


}
