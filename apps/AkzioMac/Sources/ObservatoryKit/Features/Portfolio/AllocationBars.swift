import SwiftUI

// MARK: - Allocation
//
// Actual vs target per asset, plus the drift between them. Bars slide from their old
// value; the target tick never moves unless the policy changes.
struct AllocationBars: View {
    let rows: [AllocationRow]

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        SectionCard(title: "Allocation", subtitle: "Actual vs Target") {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                ForEach(Array(rows.enumerated()), id: \.element.id) { index, row in
                    bar(row).staggeredReveal(index: index)
                }
            }
        }
    }

    private func bar(_ row: AllocationRow) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: AkzioLayout.s2) {
                Text(L10n.text(row.label, language: language)).akzioMono(11, color: AkzioColor.primaryText)
                Spacer(minLength: AkzioLayout.s2)
                Text(PpmFormatter.share(ppm: row.actualPpm, fractionDigits: 1))
                    .akzioMono(11, color: AkzioColor.primaryText)
                    .akzioNumeric(row.actualPpm, policy: policy)
                Text(PpmFormatter.percent(ppm: row.deltaPpm))
                    .akzioMono(10, color: row.isOverweight ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                    .frame(width: 56, alignment: .trailing)
            }
            GeometryReader { proxy in
                let width = proxy.size.width
                let actual = CGFloat(PpmFormatter.fraction(ppm: row.actualPpm) ?? 0)
                let target = CGFloat(PpmFormatter.fraction(ppm: row.targetPpm) ?? 0)
                ZStack(alignment: .leading) {
                    Capsule().fill(Color.white.opacity(0.05))
                    Capsule()
                        .fill(AkzioColor.goldFill)
                        .frame(width: max(2, width * actual))
                        .animation(ChartAnimation.barShift(policy), value: row.actualPpm)
                    // Target tick: the policy, drawn as a hairline the bar must reach.
                    Rectangle()
                        .fill(AkzioColor.primaryText.opacity(0.55))
                        .frame(width: 1.4, height: 12)
                        .offset(x: width * target - 0.7)
                }
            }
            .frame(height: 10)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(row.label) actual \(PpmFormatter.share(ppm: row.actualPpm))")
    }
}
