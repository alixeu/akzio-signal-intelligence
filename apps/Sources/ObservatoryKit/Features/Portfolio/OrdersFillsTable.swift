import SwiftUI

// MARK: - Orders & fills
//
// Six real `OrderReceiptState` values — the reference image's "Pending / Working"
// labels are AI-generated and are not used. A fill emits one check, then settles.
struct OrdersFillsTable: View {
    // orders/fills/verdict/reconciliation 都是执行层投影；表格只展示 receipt 状态，不推进订单生命周期。
    @Environment(\.appLanguage) private var language
    let orders: [OrderPresentation]
    let fills: [FillPresentation]
    let verdict: ExecutionVerdictKind
    let reconciliation: ReconciliationState

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        // ViewBuilder 先按 orders 是否为空选择 NoOrder 或两张表，再独立显示 reconciliation 状态。
        SectionCard(title: "Orders & Fills", subtitle: verdict.displayName) {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                if orders.isEmpty {
                    // 空 orders 是合法的 NoOrder 结果；没有可执行订单时不把 fills 缺失解释成失败。
                    // No Order is a legitimate execution outcome, not a failure.
                    StatusExplanation(.notApplicable, detail: "No executable order was produced for this run")
                } else {
                    orderTable
                    HairlineDivider()
                    fillTable
                }
                HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text("Reconciliation", language: language)).akzioText(.caption)
                    StatusBadge(reconciliation.status, size: .compact)
                }
            }
        }
    }

    private var orderTable: some View {
        // orderTable 保留每个 OrderReceiptState 的真实 displayName/status，stagger index 只负责视觉进入顺序。
        VStack(alignment: .leading, spacing: 4) {
            header(["Time", "Asset", "Side", "Type", "Qty", "Limit", "State"])
            ForEach(Array(orders.enumerated()), id: \.element.id) { index, order in
                HStack(spacing: AkzioLayout.s2) {
                    cell(order.timeLabel, width: 62)
                    cell(order.asset.rawValue, width: 52, emphasised: true)
                    PillTag(order.side.displayName, tone: order.side.tone).frame(width: 52, alignment: .leading)
                    cell(order.type, width: 44)
                    cell(PpmFormatter.quantity(micros: order.quantityMicros), width: 56)
                    cell(PpmFormatter.price(micros: order.limitPriceMicros), width: 66)
                    StatusBadge(order.state.status, size: .compact)
                        .id(order.state)
                        .transition(.opacity)
                    Spacer(minLength: 0)
                }
                .staggeredReveal(index: index)
                .animation(policy.resolve(Motion.badge), value: order.state)
            }
        }
    }

    private var fillTable: some View {
        // fillTable 只在实际 fills 存在时绘制成交行；空数组保持明确的 No fills yet 文案。
        VStack(alignment: .leading, spacing: 4) {
            header(["Time", "Asset", "Side", "Qty", "Price", "Venue"])
            if fills.isEmpty {
            // fill 的空集合是当前观察结果，不从 accepted order 推断成交。
            Text(L10n.text("No fills yet", language: language)).akzioText(.bodySmall)
            } else {
                ForEach(Array(fills.enumerated()), id: \.element.id) { index, fill in
                    HStack(spacing: AkzioLayout.s2) {
                        cell(fill.timeLabel, width: 62)
                        cell(fill.asset.rawValue, width: 52, emphasised: true)
                        PillTag(fill.side.displayName, tone: fill.side.tone).frame(width: 52, alignment: .leading)
                        cell(PpmFormatter.quantity(micros: fill.quantityMicros), width: 56)
                        cell(PpmFormatter.price(micros: fill.priceMicros), width: 66)
                        cell(fill.venue, width: 110)
                        Image(systemName: "checkmark.circle")
                            .font(.system(size: 10, weight: .medium))
                            .foregroundStyle(AkzioColor.primaryGold)
                        Spacer(minLength: 0)
                    }
                    .staggeredReveal(index: index)
                }
            }
        }
    }

    private func header(_ titles: [String]) -> some View {
        // header 的 titles 与 width(for:) 配对，统一列宽但不改变订单/成交数据。
        HStack(spacing: AkzioLayout.s2) {
            ForEach(titles, id: \.self) { title in
                Text(L10n.text(title, language: language))
                    .akzioText(.caption)
                    .frame(width: width(for: title), alignment: .leading)
            }
            Spacer(minLength: 0)
        }
    }

    private func width(for title: String) -> CGFloat {
        // 列宽是纯布局映射；未知标题使用保守默认值，不影响字段内容。
        switch title {
        case "Time": 62
        case "Asset", "Side": 52
        case "Type": 44
        case "Qty": 56
        case "Limit", "Price": 66
        case "Venue": 110
        default: 78
        }
    }

    private func cell(_ text: String, width: CGFloat, emphasised: Bool = false) -> some View {
        // cell 只负责单元格字体、宽度和截断；text 已由调用方格式化并保持原始状态语义。
        Text(text)
            .akzioMono(11, color: emphasised ? AkzioColor.primaryText : AkzioColor.secondaryText)
            .frame(width: width, alignment: .leading)
            .lineLimit(1)
    }
}
