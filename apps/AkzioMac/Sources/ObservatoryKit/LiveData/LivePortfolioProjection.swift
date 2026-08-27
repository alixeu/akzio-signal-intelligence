import Foundation

extension LiveProjection {
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

}
