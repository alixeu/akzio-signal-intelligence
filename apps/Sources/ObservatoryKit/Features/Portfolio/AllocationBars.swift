import SwiftUI

// MARK: - Allocation
//
// Actual vs target per asset, plus the drift between them. Bars slide from their old
// value; the target tick never moves unless the policy changes.
struct AllocationBars: View {
    // rows 是 PortfolioPresentation 的实际/目标权重投影；subtitle 只描述当前数据窗口，不参与计算。
    let rows: [AllocationRow]
    let subtitle: String

    init(rows: [AllocationRow], subtitle: String = "Actual vs Target") {
        // 初始化只保存父级传入的展示数据；默认 subtitle 不改变 rows 的业务含义。
        self.rows = rows
        self.subtitle = subtitle
    }

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // SectionCard 的 ViewBuilder 闭包按 rows 逐项生成 bar，enumerated index 仅用于入场动画顺序。
        SectionCard(title: "Allocation", subtitle: subtitle) {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                ForEach(Array(rows.enumerated()), id: \.element.id) { index, row in
                    bar(row).staggeredReveal(index: index)
                }
            }
        }
    }

    private func bar(_ row: AllocationRow) -> some View {
        // 每根 bar 同时显示 actual、delta 和 target；所有数值都来自同一 AllocationRow 投影。
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
                // GeometryReader 闭包把当前可用宽度转换成比例像素；缺失 fraction 回退为 0，不伪造权重。
                let width = proxy.size.width
                let actual = CGFloat(PpmFormatter.fraction(ppm: row.actualPpm) ?? 0)
                let target = CGFloat(PpmFormatter.fraction(ppm: row.targetPpm) ?? 0)
                ZStack(alignment: .leading) {
                    Capsule().fill(Color.white.opacity(0.05))
                    Capsule()
                        .fill(AkzioColor.goldFill)
                        .frame(width: max(2, width * actual))
                        .animation(ChartAnimation.barShift(policy), value: row.actualPpm)
                    // target tick 是政策目标的静态参照，actual 的动效不会移动该参照本身。
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
