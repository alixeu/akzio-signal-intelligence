import Foundation

extension LiveProjection {
    static func outcome(_ payload: ObserverSnapshotPayload) -> OutcomePresentation {
        // Outcome 投影优先使用结构化 section；旧 artifact 只作为兼容回退，不能覆盖已有结构化窗口。
        let section = payload.outcome
        let analytics = section?.data
        // Outcome section、learning artifact、current-run artifact 按优先级提供来源；全都缺失时仍返回不可用 projection。
        let artifact = payload.learning.data?.artifacts.last(where: { $0.kind == "outcome" })
            ?? payload.currentRun?.artifacts.last(where: { $0.kind == "outcome" })
        let legacyWindows = artifact?.payload["windows"]?.array ?? []

        let windows = OutcomeHorizonKind.allCases.compactMap { horizon -> OutcomeWindowPresentation? in
            // compactMap 只展示能够完整解析的窗口；缺失或类型错误的 JSON 不被填成零指标。
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
            // progress/status 来自 Core 的 observed metrics；window 存在才表示 sealed evidence available。
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
                    : "Sealed outcome evidence available"
            )
        }
        // selected 只选择最后一个可用窗口；没有 sealed window 时回退 T+1，不代表 T+1 已完成。
        return OutcomePresentation(
            horizons: horizons,
            windows: windows,
            selected: windows.last?.horizon ?? .t1,
            observedTradingDays: analytics.map { Int($0.completedTradingSessions) },
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
