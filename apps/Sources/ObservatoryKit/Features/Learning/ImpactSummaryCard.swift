import SwiftUI

// MARK: - Impact
//
// What the learning loop actually produced: money, lessons, evolved policies and the
// areas they touched. Counts are real counts, so zero is a legitimate value here.
struct ImpactSummaryCard: View {
    // impact 是 sealed outcome 归因后的展示模型；zero count 是合法结果，不能用空态替代。
    let impact: ImpactSummaryPresentation

    // policy 只控制 count-up，language 负责标题本地化，不参与 impact 数值计算。
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // totalImpactMicros 的 map/?? 把可选金额转换成展示字符串和动画数值；缺失时保持 Unavailable。
        SectionCard(title: "Impact", subtitle: "Attributed to sealed outcomes") {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                HStack(spacing: AkzioLayout.s4) {
                    headline(
                        "Attributed Utility",
                    impact.totalImpactMicros.map {
                        PpmFormatter.currency(micros: $0, signed: true)
                    } ?? MissingValue.unavailable.rawValue,
                    Double(impact.totalImpactMicros ?? 0) / PpmFormatter.ppmPerUnit,
                    tone: impact.totalImpactPpm >= 0 ? .gold : .coral
                )
                    headline(
                        "Lessons Created",
                        PpmFormatter.count(impact.lessonsCreated),
                        Double(impact.lessonsCreated),
                        tone: .neutral,
                        delta: impact.lessonsDelta
                    )
                    headline(
                        "Policies Evolved",
                        PpmFormatter.count(impact.policiesEvolved),
                        Double(impact.policiesEvolved),
                        tone: .neutral,
                        delta: impact.policiesDelta
                    )
                }
                HairlineDivider()
            Text(L10n.text("Top Impact Areas", language: language)).akzioText(.caption)
                ForEach(Array(impact.areas.enumerated()), id: \.element.id) { index, area in
                    // area 的 impactPpm 只决定比例条和正负 tone，label 与数值仍来自同一 Rust 投影。
                    HStack(spacing: AkzioLayout.s2) {
                        Text(area.label).akzioText(.bodySmall).frame(width: 96, alignment: .leading)
                        RatioBar(
                            fraction: min(abs(Double(area.impactPpm)) / 20_000, 1),
                            tone: area.impactPpm >= 0 ? .gold : .coral,
                            height: 5
                        )
                        Text(PpmFormatter.percent(ppm: area.impactPpm))
                            .akzioMono(10, color: area.impactPpm >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                            .frame(width: 62, alignment: .trailing)
                    }
                    .staggeredReveal(index: index)
                }
            }
        }
    }

    private func headline(
        _ label: String,
        _ value: String,
        _ numeric: Double,
        tone: AkzioTone,
        delta: Int? = nil
    ) -> some View {
        // delta 是可选窗口比较值；nil 不渲染变化行，0 明确显示 No change 而不是伪造增量。
        VStack(alignment: .leading, spacing: 3) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value)
                .akzioMetric(22, color: tone == .neutral ? AkzioColor.primaryText : tone.color)
                .akzioCountUp(numeric, policy: policy)
            if let delta {
                Text(delta == 0
                    ? L10n.text("No change", language: language)
                    : "+\(delta) \(L10n.text("this window", language: language))")
                    .akzioMono(10, color: AkzioColor.mutedText)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
