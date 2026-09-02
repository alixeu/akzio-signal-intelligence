import SwiftUI

// MARK: - Orders & fills
//
// Six real `OrderReceiptState` values — the reference image's "Pending / Working"
// labels are AI-generated and are not used. A fill emits one check, then settles.
struct OrdersFillsTable: View {
    @Environment(\.appLanguage) private var language
    let orders: [OrderPresentation]
    let fills: [FillPresentation]
    let verdict: ExecutionVerdictKind
    let reconciliation: ReconciliationState

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        SectionCard(title: "Orders & Fills", subtitle: verdict.displayName) {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                if orders.isEmpty {
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
        VStack(alignment: .leading, spacing: 4) {
            header(["Time", "Asset", "Side", "Qty", "Price", "Venue"])
            if fills.isEmpty {
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
        Text(text)
            .akzioMono(11, color: emphasised ? AkzioColor.primaryText : AkzioColor.secondaryText)
            .frame(width: width, alignment: .leading)
            .lineLimit(1)
    }
}
