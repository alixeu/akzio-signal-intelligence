import Foundation

extension LiveProjection {
    static func portfolio(_ payload: ObserverSnapshotPayload) -> PortfolioPresentation {
        // Portfolio 只把 Observer 的账户/执行 Artifact 投影成 UI；缺少 portfolio.data 时直接返回显式 unavailable。
        guard let portfolio = payload.portfolio.data else { return unavailablePortfolio }
        // currentRun/artifact 均是 Optional 来源；last/filter 只选择已持久化的最新记录，不在 UI 层创建执行状态。
        let currentArtifacts = payload.currentRun?.artifacts ?? []
        let plan = currentArtifacts.last(where: { $0.kind == "execution_plan" })
        let isPositionPlan = payload.currentRun?.workflow.run.purpose
            == RunPurpose.positionPlan.rawValue
        let positionPlan = isPositionPlan
            ? currentArtifacts.last(where: { $0.kind == "decision_context" })
            : nil
        let receipts = currentArtifacts.filter { $0.kind == "order_receipt" }
        let reconciliationArtifact = currentArtifacts.last(where: { $0.kind == "reconciliation" })
        let targetWeights: [String: JSONValue] = if isPositionPlan {
            // PositionPlan 展示 research_plan.validated 的研究目标，明确 execution N/A；Paper 才读取 execution_plan。
            Self.researchTargetWeights(positionPlan?.payload["research_plan"]?["validated"])
        } else {
            plan?.payload["target"]?["weights"]?.object ?? [:]
        }
        let positions = portfolio.positions.compactMap { position -> PositionPresentation? in
            // 未知 symbol 被丢弃而不是映射到错误资产；actual/target 的整数比例保留 Optional 缺失边界。
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
        let positionsByAsset = Dictionary(uniqueKeysWithValues: positions.map { ($0.asset, $0) })
        let allocations = TradableAsset.allCases.map { asset in
            AllocationRow(
                label: asset.rawValue,
                actualPpm: positionsByAsset[asset]?.actualPpm ?? 0,
                targetPpm: targetWeights[asset.rawValue.lowercased()]?.int ?? 0
            )
        }
        let planOrders = plan?.payload["orders"]?.array ?? []
        let orders = receipts.compactMap { receipt -> OrderPresentation? in
            // Receipt 只有在资产和对应 plan order 都能解析时才形成 UI 行；缺失 side/state 使用既有失败/买入回退。
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
        // UI 只依据已持久化 verdict/reconciliation Artifact 的形状展示状态，不把 receipt 数量当成成交证明。
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
            AllocationFlowStage(
                title: "Plan",
                symbol: "list.bullet.rectangle",
                isActive: plan != nil || positionPlan != nil
            ),
            AllocationFlowStage(title: "Broker", symbol: "building.columns", isActive: !receipts.isEmpty),
            AllocationFlowStage(
                title: "Reconcile",
                symbol: "arrow.triangle.2.circlepath",
                isActive: reconciliationArtifact != nil
            ),
        ]
        let leverage = plan?.payload["factor_exposure"]?["leveraged_equity_ppm"]?.int
        let analytics = portfolio.analytics?.data
        // 返回的 PortfolioPresentation 保留 live 投影中的 Optional 风险、成交和曲线边界；空 curve 不代表收益为零。
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
            reconciliation: reconciliation,
            allocationSubtitle: isPositionPlan
                ? "Research target · execution N/A"
                : "Actual vs Target"
        )
    }

    private static func researchTargetWeights(_ plan: JSONValue?) -> [String: JSONValue] {
        // research allocation 是开放 JSON 的可选投影；只收集有 asset 和 target_weight_ppm 的行并按小写 key 索引。
        guard let rows = plan?["allocations"]?.array else { return [:] }
        var result: [String: JSONValue] = [:]
        for row in rows {
            guard let asset = row["asset"]?.string,
                  let weight = row["target_weight_ppm"]
            else { continue }
            result[asset.lowercased()] = weight
        }
        return result
    }

}
