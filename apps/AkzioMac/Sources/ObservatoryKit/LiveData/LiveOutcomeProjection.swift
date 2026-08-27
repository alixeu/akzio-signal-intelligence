import Foundation

extension LiveProjection {
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


}
