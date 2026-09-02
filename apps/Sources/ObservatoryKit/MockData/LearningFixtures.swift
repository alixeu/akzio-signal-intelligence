import Foundation

// MARK: - Learning fixtures
//
// Retrospectives are outcome-backed: a scenario with no sealed horizon has none.
enum LearningFixtures {
    static func cards(scenario: MockScenario) -> [RetrospectiveCardPresentation] {
        guard scenario.purpose.isCanonical, !scenario.sealedHorizons.isEmpty else { return [] }
        var generator = SeededGenerator(seed: scenario.seed &+ 811)
        let blueprint: [(String, RetrospectiveConclusion, [RetrospectiveCategory], Double)] = [
            ("Momentum entry held through the open", .worked, [.research, .decision], 0.021),
            ("Semiconductor hedge lagged the move", .failed, [.risk, .execution], -0.014),
            ("Evidence set was thinner than usual", .mixed, [.evidence], 0.004),
            ("Turnover budget forced a partial rotation", .unresolved, [.execution, .contract], -0.006),
        ]
        let degradedIndex = scenario == .retrospectiveMixed ? 2 : -1

        return blueprint.enumerated().map { index, entry in
            let impactPpm = Int(entry.3 * PpmFormatter.ppmPerUnit)
            let degraded = index == degradedIndex
            return RetrospectiveCardPresentation(
                id: "retro-\(scenario.code)-\(index + 1)",
                title: entry.0,
                dateLabel: dateLabel(daysAgo: index + 1),
                conclusion: degraded ? .unresolved : entry.1,
                status: degraded ? .modelUnavailable : .complete,
                categories: entry.2,
                pnlMicros: degraded ? nil : Int64(entry.3 * CurveFixtures.baseEquity * PpmFormatter.ppmPerUnit),
                impactPpm: degraded ? nil : impactPpm,
                spark: CurveFixtures.retrospectiveSpark(scenario: scenario, index: index, trend: entry.3 * 6),
                counterfactual: degraded
                    ? ""
                    : "Holding the prior weights would have returned \(PpmFormatter.percent(ppm: impactPpm / 2)).",
                lessonCandidate: degraded ? "" : "Size the hedge from realized dispersion, not headline volatility.",
                diagnosticGaps: degraded ? ["Retrospective model unavailable"] : [],
                tags: entry.2.map(\.displayName),
                impact: generator.bool(probability: 0.4) ? .notable : .info
            )
        }
    }

    static func timeline(scenario: MockScenario) -> [TimelineNodePresentation] {
        var nodes: [TimelineNodePresentation] = [
            TimelineNodePresentation(
                id: "tl-event",
                kind: .event,
                label: "Session opened",
                dateLabel: dateLabel(daysAgo: 5),
                detail: "Market clock confirmed a regular session.",
                position: 0.05,
                isCurrent: false
            ),
            TimelineNodePresentation(
                id: "tl-decision",
                kind: .decision,
                label: "Allocation decided",
                dateLabel: dateLabel(daysAgo: 5),
                detail: "Synthesizer proposal accepted by the decision gate.",
                position: 0.32,
                isCurrent: false
            ),
        ]
        guard !scenario.sealedHorizons.isEmpty else { return nodes }
        nodes.append(
            TimelineNodePresentation(
                id: "tl-outcome",
                kind: .outcome,
                label: "T+1 sealed",
                dateLabel: dateLabel(daysAgo: 4),
                detail: "Outcome window sealed with complete evidence.",
                position: 0.61,
                isCurrent: scenario.sealedHorizons.count == 1
            )
        )
        nodes.append(
            TimelineNodePresentation(
                id: "tl-lesson",
                kind: .lesson,
                label: "Lesson recorded",
                dateLabel: dateLabel(daysAgo: 3),
                detail: "Hedge sizing rule promoted to a candidate policy.",
                position: 0.88,
                isCurrent: scenario.sealedHorizons.count > 1
            )
        )
        return nodes
    }

    static func policyTracks(scenario: MockScenario) -> [PolicyTrackPresentation] {
        guard scenario.purpose.isCanonical else { return [] }
        var generator = SeededGenerator(seed: scenario.seed &+ 829)

        func exposure(_ state: CandidatePolicyState) -> Int {
            switch state {
            case .candidate: 0
            case .canary10: 100_000
            case .canary25: 250_000
            case .canary50: 500_000
            case .active: 1_000_000
            }
        }

        func track(
            _ subject: PolicySubjectKind,
            _ name: String,
            memory: MemoryLifecycle?,
            candidate: CandidatePolicyState?
        ) -> PolicyTrackPresentation {
            PolicyTrackPresentation(
                subject: subject,
                name: name,
                memoryState: memory,
                candidateState: candidate,
                activeSinceLabel: dateLabel(daysAgo: generator.int(in: 6...28)),
                winRatePpm: scenario.dataUnavailable ? nil : generator.int(in: 420_000...760_000),
                netImpactPpm: scenario.dataUnavailable ? nil : generator.int(in: -8_000...26_000),
                stabilityPpm: generator.int(in: 620_000...960_000),
                exposurePpm: candidate.map(exposure)
            )
        }

        return [
            track(.memory, "Hedge sizing from dispersion", memory: scenario.memoryLifecycle, candidate: nil),
            track(.contract, "Analyst evidence floor", memory: nil, candidate: scenario.candidateState),
            track(.topology, "Three-analyst fan-out", memory: nil, candidate: .active),
        ]
    }

    static func impact(scenario: MockScenario) -> ImpactSummaryPresentation {
        var generator = SeededGenerator(seed: scenario.seed &+ 853)
        let items = cards(scenario: scenario)
        let totalPpm = items.compactMap(\.impactPpm).reduce(0, +)
        let totalMicros = Double(totalPpm) / PpmFormatter.ppmPerUnit
            * CurveFixtures.baseEquity * PpmFormatter.ppmPerUnit
        return ImpactSummaryPresentation(
            totalImpactMicros: Int64(totalMicros),
            totalImpactPpm: totalPpm,
            lessonsCreated: items.filter { !$0.lessonCandidate.isEmpty }.count,
            lessonsDelta: items.isEmpty ? 0 : generator.int(in: 0...2),
            policiesEvolved: policyTracks(scenario: scenario).filter { $0.memoryState != .candidate }.count,
            policiesDelta: items.isEmpty ? 0 : 1,
            areas: RetrospectiveCategory.allCases.map { category in
                ImpactAreaPresentation(
                    label: category.displayName,
                    impactPpm: items.isEmpty ? 0 : generator.int(in: -6_000...18_000)
                )
            }
        )
    }

    static func learning(scenario: MockScenario) -> LearningPresentation {
        LearningPresentation(
            cards: cards(scenario: scenario),
            timeline: timeline(scenario: scenario),
            policyTracks: policyTracks(scenario: scenario),
            impact: impact(scenario: scenario),
            activePolicyName: "Hedge sizing from dispersion",
            timeRangeLabel: "Last 30 Trading Sessions"
        )
    }

    /// Labels are derived from the frozen anchor, never from the wall clock.
    /// ponytail: weekday-only fixture calendar; replace with broker sessions when holidays matter.
    static func sessionDate(sessionsAgo: Int) -> Date {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "America/New_York")!
        var date = ObservatorySnapshot.anchor
        var remaining = max(0, sessionsAgo)
        while remaining > 0 {
            date = calendar.date(byAdding: .day, value: -1, to: date)!
            let weekday = calendar.component(.weekday, from: date)
            if (2...6).contains(weekday) { remaining -= 1 }
        }
        return date
    }

    static func dateLabel(daysAgo: Int) -> String {
        let date = sessionDate(sessionsAgo: daysAgo)
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        formatter.dateFormat = "MMM d"
        return formatter.string(from: date)
    }
}
