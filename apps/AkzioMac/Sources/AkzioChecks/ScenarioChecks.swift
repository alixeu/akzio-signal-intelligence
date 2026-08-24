import Foundation
import ObservatoryKit

/// Locks the load-bearing rules of the 20 mock scenarios: determinism, sealing,
/// Not Triggered / Not Applicable / Unavailable semantics, and no fake zeroes.
func runScenarioChecks() {
    Check.suite("Scenarios — coverage") {
        Check.equal(MockScenario.allCases.count, 20, "scenario count")
        let codes = Set(MockScenario.allCases.map(\.code))
        Check.equal(codes.count, 20, "distinct codes")
        let seeds = Set(MockScenario.allCases.map(\.seed))
        Check.equal(seeds.count, 20, "distinct seeds")
        for scenario in MockScenario.allCases {
            Check.expect(!scenario.routes.isEmpty, "\(scenario.code) declares capture routes")
            Check.expect(!scenario.title.isEmpty, "\(scenario.code) has a title")
        }
    }

    Check.suite("Scenarios — determinism") {
        for scenario in MockScenario.allCases {
            let first = ScenarioLibrary.build(scenario)
            let second = ScenarioLibrary.build(scenario)
            Check.equal(
                first.determinismFingerprint,
                second.determinismFingerprint,
                "\(scenario.code) rebuilds identically"
            )
            Check.equal(
                first.anchor.timeIntervalSince1970,
                ObservatorySnapshot.anchor.timeIntervalSince1970,
                "\(scenario.code) uses the frozen anchor"
            )
            Check.equal(first.run.brokerSession, "2026-08-19", "\(scenario.code) broker session")
        }
    }

    Check.suite("Scenarios — workflow semantics") {
        for scenario in MockScenario.allCases {
            let snapshot = ScenarioLibrary.snapshot(scenario)
            let nodes = snapshot.workflow.nodes

            // The Critic is the only optional stage: skipped means Not Triggered.
            if let critic = nodes.first(where: { $0.stage == .critic }) {
                if scenario.criticTriggered {
                    Check.expect(critic.status != .notTriggered, "\(scenario.code) critic runs")
                } else {
                    Check.equal(critic.status, .notTriggered, "\(scenario.code) critic not triggered")
                }
            } else {
                Check.expect(false, "\(scenario.code) has a critic node")
            }

            // Paper Commit is Not Applicable outside canonical Paper runs.
            if let commit = nodes.first(where: { $0.stage == .paperCommit }) {
                if scenario.purpose.submitsPaperOrders {
                    Check.expect(commit.status != .notApplicable, "\(scenario.code) paper commit applies")
                } else {
                    Check.equal(commit.status, .notApplicable, "\(scenario.code) paper commit N/A")
                }
            }

            // At most one stage may be running at a time.
            Check.expect(snapshot.workflow.activeCount <= 1, "\(scenario.code) single active stage")

            // Every edge references a real node.
            let ids = Set(nodes.map(\.id))
            for edge in snapshot.workflow.edges {
                Check.expect(ids.contains(edge.from), "\(scenario.code) edge source \(edge.from)")
                Check.expect(ids.contains(edge.to), "\(scenario.code) edge target \(edge.to)")
            }
        }
    }

    Check.suite("Scenarios — outcome sealing") {
        for scenario in MockScenario.allCases {
            let outcome = ScenarioLibrary.snapshot(scenario).outcome
            Check.equal(outcome.horizons.count, 3, "\(scenario.code) three horizons")
            Check.expect(outcome.observingCount <= 1, "\(scenario.code) at most one observing ring")
            for horizon in outcome.horizons {
                Check.expect(horizon.isConsistent, "\(scenario.code) \(horizon.id) sealing consistent")
                if !horizon.isSealed {
                    Check.expect(
                        horizon.status != .completed,
                        "\(scenario.code) \(horizon.id) unsealed is never Completed"
                    )
                }
                if horizon.status == .waiting {
                    Check.expect(
                        horizon.progress == nil,
                        "\(scenario.code) \(horizon.id) waiting ring draws no progress"
                    )
                }
            }
            // A window exists only for a sealed horizon.
            for window in outcome.windows {
                Check.expect(
                    scenario.sealedHorizons.contains(window.horizon),
                    "\(scenario.code) window \(window.id) is sealed"
                )
            }
            Check.equal(outcome.windows.count, scenario.sealedHorizons.count, "\(scenario.code) window count")
        }
    }

    Check.suite("Scenarios — cross-page lifecycle") {
        for scenario in MockScenario.allCases {
            let snapshot = ScenarioLibrary.snapshot(scenario)
            let sealed = snapshot.outcome.horizons.filter(\.isSealed)
            let activeStage = snapshot.workflow.activeStageID
                .flatMap { snapshot.workflow.node(id: $0)?.stage }
            let isOutcomeObservation: Bool
            if case .horizon = activeStage {
                isOutcomeObservation = true
            } else {
                isOutcomeObservation = false
            }

            if activeStage != nil, !isOutcomeObservation {
                Check.expect(
                    sealed.isEmpty,
                    "\(scenario.code) active workflow cannot already have a sealed outcome"
                )
                Check.expect(
                    snapshot.learning.cards.isEmpty,
                    "\(scenario.code) active workflow cannot already have current-run retrospectives"
                )
            }

            if !scenario.purpose.isCanonical {
                Check.expect(
                    sealed.isEmpty,
                    "\(scenario.code) noncanonical run cannot seal outcomes"
                )
                Check.expect(
                    snapshot.learning.cards.isEmpty,
                    "\(scenario.code) noncanonical run cannot promote retrospectives"
                )
            }

            if snapshot.workflow.activeStageID == WorkflowStageKind.synthesizer.id {
                Check.expect(
                    snapshot.portfolio.orders.isEmpty,
                    "\(scenario.code) synthesizer-stage run cannot already have broker orders"
                )
            }
        }
    }

    Check.suite("Scenarios — execution & missing values") {
        for scenario in MockScenario.allCases {
            let snapshot = ScenarioLibrary.snapshot(scenario)
            let portfolio = snapshot.portfolio

            if scenario.isDecisionBlocked || !scenario.hasOrders {
                Check.expect(portfolio.orders.isEmpty, "\(scenario.code) no orders submitted")
                Check.equal(portfolio.verdict, .noOrder, "\(scenario.code) verdict is No Order")
            } else {
                Check.expect(!portfolio.orders.isEmpty, "\(scenario.code) has orders")
                Check.equal(portfolio.verdict, .accepted, "\(scenario.code) verdict accepted")
            }

            if scenario.allFilled {
                Check.expect(
                    portfolio.orders.allSatisfy { $0.state == .filled },
                    "\(scenario.code) all orders filled"
                )
                Check.equal(portfolio.reconciliation, .complete, "\(scenario.code) reconciliation complete")
            }
            if scenario.hasPartialFill {
                Check.expect(
                    portfolio.orders.contains { $0.state == .partiallyFilled },
                    "\(scenario.code) has a partial fill"
                )
                Check.equal(portfolio.reconciliation, .partial, "\(scenario.code) reconciliation partial")
            }

            // Fills never exceed the order book.
            Check.expect(
                portfolio.fills.count <= portfolio.orders.count,
                "\(scenario.code) fills bounded by orders"
            )

            if scenario.dataUnavailable {
                Check.expect(portfolio.risk.betaPpm == nil, "\(scenario.code) risk beta unavailable")
                Check.expect(portfolio.unrealizedPnlPpm == nil, "\(scenario.code) unrealized unavailable")
                for metric in snapshot.health {
                    Check.equal(metric.value, "Unavailable", "\(scenario.code) health value hidden")
                    Check.expect(metric.fraction == nil, "\(scenario.code) health gauge empty, not zero")
                }
            }
            if scenario.dataStale {
                Check.equal(snapshot.run.dataStatus, .stale, "\(scenario.code) data reads stale")
            }
        }
    }

    Check.suite("Scenarios — learning & archive") {
        let canaryRows = MockScenario.allCases.compactMap { scenario in
            ScenarioLibrary.snapshot(scenario).archive.rows.first {
                $0.runID == ArchiveFixtures.canaryRunID
            }
        }
        Check.equal(canaryRows.count, MockScenario.allCases.count, "canary appears in every archive fixture")
        Check.equal(
            Set(canaryRows.map(\.resultPpm)).count,
            1,
            "same canary run keeps one result across scenarios"
        )

        for scenario in MockScenario.allCases {
            let snapshot = ScenarioLibrary.snapshot(scenario)
            // Retrospectives require a sealed outcome.
            if scenario.sealedHorizons.isEmpty {
                Check.expect(snapshot.learning.cards.isEmpty, "\(scenario.code) no unsourced retrospectives")
            } else {
                Check.expect(!snapshot.learning.cards.isEmpty, "\(scenario.code) has retrospectives")
            }
            for card in snapshot.learning.cards where card.isDegraded {
                Check.expect(card.impactPpm == nil, "\(scenario.code) degraded card hides impact")
                Check.expect(card.lessonCandidate.isEmpty, "\(scenario.code) degraded card invents no lesson")
            }
            let archive = snapshot.archive
            Check.equal(archive.rows.count, archive.pageSize, "\(scenario.code) archive page is full")
            Check.expect(archive.totalRuns >= archive.rows.count, "\(scenario.code) archive total sane")
            for row in archive.rows where !row.purpose.isCanonical {
                Check.expect(row.resultPpm == nil, "\(scenario.code) noncanonical run has no outcome number")
            }
            Check.expect(
                archive.rows.contains { $0.runID == ArchiveFixtures.canaryRunID },
                "\(scenario.code) archive keeps the real Paper canary row"
            )
            if scenario == .archiveFilteredResults {
                Check.expect(!archive.activeFilters.isEmpty, "\(scenario.code) filters are visible")
            }
        }
    }

    Check.suite("Archive — sorting, paging and row ownership") {
        let rows = ScenarioLibrary.snapshot(.paperRunningSynthesizerActive).archive.rows
            + ScenarioLibrary.snapshot(.debugCompleted).archive.rows
        let ascending = ArchiveQuery.sorted(rows, by: .started, ascending: true)
        let descending = ArchiveQuery.sorted(rows, by: .started, ascending: false)
        Check.expect(
            zip(ascending, ascending.dropFirst()).allSatisfy { $0.startedAt <= $1.startedAt },
            "started ascending uses typed dates"
        )
        Check.expect(
            zip(descending, descending.dropFirst()).allSatisfy { $0.startedAt >= $1.startedAt },
            "started descending is a strict reverse ordering"
        )

        let firstPage = ArchiveQuery.page(rows, number: 1, size: 12)
        let secondPage = ArchiveQuery.page(rows, number: 2, size: 12)
        Check.equal(firstPage.count, 12, "first page size")
        Check.equal(secondPage.count, 12, "second page size")
        Check.expect(
            Set(firstPage.map(\.id)).isDisjoint(with: Set(secondPage.map(\.id))),
            "changing page changes rows"
        )

        let canary = rows.first { $0.runID == ArchiveFixtures.canaryRunID }!
        Check.expect(
            canary.stageProgress.allSatisfy { $0.status == .succeeded },
            "completed canary preview owns completed stage progress"
        )
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "America/New_York")!
        for row in rows {
            Check.expect(
                (2...6).contains(calendar.component(.weekday, from: row.startedAt)),
                "\(row.id) uses a broker weekday"
            )
        }
    }

    Check.suite("Scenarios — settings") {
        for scenario in MockScenario.allCases {
            let settings = ScenarioLibrary.settings(scenario)
            Check.equal(
                settings.reduceMotionOverride,
                scenario.reduceMotionPreferred,
                "\(scenario.code) reduce-motion override"
            )
        }
    }
}
