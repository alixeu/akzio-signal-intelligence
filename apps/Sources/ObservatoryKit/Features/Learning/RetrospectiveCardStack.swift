import SwiftUI

// MARK: - Retrospective stack
//
// One readable retrospective at a time, with explicit previous/next controls.
struct RetrospectiveCardStack: View {
    // cards 是按筛选结果传入的 sealed retrospective 投影；index 与 showsCounterfactual 由父级 @State 通过 Binding 管理。
    let cards: [RetrospectiveCardPresentation]
    @Binding var index: Int
    @Binding var showsCounterfactual: Bool

    @Environment(\.motionPolicy) private var policy
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language

    var body: some View {
        // ViewBuilder 根据 cards 是否为空选择等待说明或可浏览的 stack/controls，不为缺失 outcome 生成卡片。
        if cards.isEmpty {
            SectionCard(title: "Retrospective") {
                StatusExplanation(.waiting, detail: "Retrospectives require a sealed Paper outcome")
            }
        } else {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                stack
                controls
            }
        }
    }

    private var stack: some View {
        // clamp 防止过滤器改变后 index 越界；当前卡片仍保留固定高度以稳定邻近布局。
        // Translucent cards must not overlap: neighbouring text remains visible
        // through the material even when the current card has a higher zIndex.
        cardView(cards[min(max(index, 0), cards.count - 1)], isCurrent: true)
            .frame(height: 268)
    }

    private func cardView(_ card: RetrospectiveCardPresentation, isCurrent: Bool) -> some View {
        // degraded 卡片保留数值和 diagnostic gaps，只隐藏不可验证的结论性文案。
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: card.conclusion.symbol)
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(card.conclusion.tone.color)
            .sharedElement(isCurrent ? .retrospectiveBadge : .noSharedElement, in: isCurrent ? namespace : nil)
                VStack(alignment: .leading, spacing: 1) {
                    Text(card.title).akzioText(.sectionTitle).lineLimit(2)
                    Text("\(card.dateLabel) · \(L10n.text(card.conclusion.displayName, language: language))").akzioText(.caption)
                }
                Spacer(minLength: AkzioLayout.s2)
                if card.isDegraded {
                    PillTag(RetrospectiveStatus.modelUnavailable.displayName, tone: .muted)
                }
            }
            HairlineDivider()
            HStack(alignment: .top, spacing: AkzioLayout.s4) {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    if card.isDegraded {
                        // Outcome numbers survive; invented conclusions do not.
                        StatusExplanation(.unavailable, detail: "Retrospective model was unavailable for this run")
                        ForEach(card.diagnosticGaps, id: \.self) { gap in
                            Text(gap).akzioText(.bodySmall, color: AkzioColor.mutedText)
                        }
                    } else {
                        labelled("Counterfactual", card.counterfactual)
                        labelled("Lesson Candidate", card.lessonCandidate)
                    }
                    HStack(spacing: 5) {
                        ForEach(card.tags, id: \.self) { tag in
                            PillTag(tag, tone: .neutral)
                        }
                    }
                }
                VStack(alignment: .trailing, spacing: AkzioLayout.s2) {
                    Text(PpmFormatter.currency(micros: card.pnlMicros, signed: true))
                        .akzioMetric(18, color: (card.impactPpm ?? 0) >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                    Text(PpmFormatter.percent(ppm: card.impactPpm)).akzioMono(11)
                    MiniSparkline(
                        values: card.spark,
                        tone: (card.impactPpm ?? 0) >= 0 ? .gold : .coral
                    )
                    .frame(width: 148, height: 40)
                }
            }
            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioCard(
            border: card.isDegraded ? AkzioColor.hairline : AkzioColor.goldHairline
        )
    }

    private func labelled(_ title: String, _ body: String) -> some View {
        // labelled 是纯展示 helper；title/body 已由数据投影决定，组件不负责翻译或推断。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(title, language: language)).akzioText(.caption)
            Text(body).akzioText(.bodySmall)
        }
    }

    private var controls: some View {
        // 两个 Button 的闭包只调用受边界保护的 step；详情按钮独立切换本地 showsCounterfactual。
        HStack(spacing: AkzioLayout.s2) {
            Button { step(-1) } label: { Image(systemName: "chevron.left") }
                .buttonStyle(PressableButtonStyle())
                .disabled(index <= 0)
            Button { step(1) } label: { Image(systemName: "chevron.right") }
                .buttonStyle(PressableButtonStyle())
                .disabled(index >= cards.count - 1)
            Text("\(index + 1) / \(cards.count)").akzioMono(11, color: AkzioColor.mutedText)
            Spacer(minLength: AkzioLayout.s2)
            Button {
                withAnimation(policy.resolve(Motion.panel)) { showsCounterfactual.toggle() }
            } label: {
                Text(L10n.text(showsCounterfactual ? "Hide Detail" : "Show Detail", language: language))
                    .akzioText(.label, color: AkzioColor.primaryGold)
            }
            .buttonStyle(PressableButtonStyle())
        }
        .font(.system(size: 10, weight: .semibold))
        .foregroundStyle(AkzioColor.secondaryText)
    }

    private func step(_ delta: Int) {
        // step 在动画闭包内更新 Binding，并把索引限制在 0...cards.count-1，避免空数组被访问。
        withAnimation(policy.resolve(.spring(response: 0.48, dampingFraction: 0.95))) {
            index = min(max(index + delta, 0), cards.count - 1)
        }
    }
}
