import SwiftUI

// MARK: - Learning
//
// Five tabs over the same sealed-outcome evidence. Filters re-rank the retrospective
// stack; a degraded (`ModelUnavailable`) card keeps its numbers and drops its prose.
struct LearningPage: View {
    // store 提供 sealed outcome 与研究审计的展示投影；页面不自行创建 Lesson 或学习结论。
    let store: ObservatoryStore

    // 这些 Environment 值只控制命名空间、动效和文案，不改变 learning 数据本身。
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var cardIndex = 0
    @State private var showsCounterfactual = false
    @State private var activeCategory: RetrospectiveCategory?

    // 每次 body 重算都从当前 store 投影读取，避免把旧的过滤结果当成持久状态。
    private var learning: LearningPresentation { store.displayLearning }

    private var filteredCards: [RetrospectiveCardPresentation] {
        // nil 表示未选过滤器；有类别时仅保留包含该类别的卡片，顺序仍由 Rust 投影决定。
        guard let activeCategory else { return learning.cards }
        return learning.cards.filter { $0.categories.contains(activeCategory) }
    }

    var body: some View {
        // 主体和 toolbar 都是 ViewBuilder 闭包；未完成的 learning 只显示状态，不渲染可误解的 retrospective 内容。
        PageScaffold(route: .learning) {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                if !learning.researchAudit.isEmpty {
                    SectionCard(title: "本次运行的 Lesson 召回与重验") {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("召回记录与重验建议来自本次研究；不代表已完成 Outcome 学习或变更 Active Lesson。")
                                .akzioText(.caption, color: AkzioColor.mutedText)
                            ForEach(learning.researchAudit) { row in
                                Text(row.title).akzioText(.bodySmall)
                                Text(row.detail).akzioText(.caption, color: AkzioColor.mutedText).textSelection(.enabled)
                            }
                        }
                    }
                }
                if learning.availabilityStatus == .completed {
                    // completed 才启用筛选和内容区；其他状态沿 pendingLearning 的证据边界展示。
                    StagedSection(index: 0) { filters }
                    StagedSection(index: 1) { content }
                } else {
                    StagedSection(index: 0) { pendingLearning }
                }
            }
        } toolbar: {
            // Binding 的 get/set 直连 store.learningTab；控件在没有完成学习证据时保持禁用。
            AkzioSegmentedControl(
                selection: Binding(
                    get: { store.learningTab },
                    set: { store.learningTab = $0 }
                ),
                options: LearningPresentation.Tab.allCases.map { (value: $0, label: $0.displayName) }
            )
            .disabled(learning.availabilityStatus != .completed)
        }
    }

    private var pendingLearning: some View {
        // pending 卡片区分 status、reason 和固定的 Debug 不产生 learning 说明，不把等待当作已学习。
        SectionCard(title: L10n.text("Learning Evidence", language: language)) {
            VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                StatusBadge(learning.availabilityStatus)
                StatusExplanation(
                    learning.availabilityStatus,
                    detail: L10n.text(
                        learning.availabilityReason ?? "No canonical learning artifacts are available yet",
                        language: language
                    )
                )
                Text(L10n.text(
                    "Learning appears only after real Paper outcomes are sealed; Debug runs never create learning data.",
                    language: language
                ))
                .akzioText(.bodySmall, color: AkzioColor.secondaryText)
            }
        }
    }

    // MARK: Filters

    private var filters: some View {
        // filters 只改变本地 @State 的 activeCategory/cardIndex，并通过 policy 统一控制切换动画。
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(learning.timeRangeLabel, language: language)).akzioText(.label)
            HairlineDivider(.vertical).frame(height: 14)
            ForEach(RetrospectiveCategory.allCases) { category in
                Chip(
                    category.displayName,
                    kind: .filter,
                    isSelected: activeCategory == category
                ) {
                    // 同一类别再次点击清除筛选；每次筛选变化都把浏览位置归零。
                    withAnimation(policy.resolve(Motion.control)) {
                        activeCategory = activeCategory == category ? nil : category
                        cardIndex = 0
                    }
                }
            }
            Spacer(minLength: AkzioLayout.s2)
            Text("\(filteredCards.count) / \(learning.cards.count) \(L10n.text("retrospectives", language: language))")
                .akzioMono(10, color: AkzioColor.mutedText)
        }
    }

    // MARK: Tabs

    @ViewBuilder
    private var content: some View {
        // ViewBuilder switch 根据 store 的 tab 选择同一 learning 投影的五种视图，不改变底层数据。
        switch store.learningTab {
        case .retrospective:
            HStack(alignment: .top, spacing: AkzioLayout.s4) {
                RetrospectiveCardStack(
                    cards: filteredCards,
                    index: Binding(
                        // 栈组件读写 cardIndex，但 get 会把越界值夹回当前 filteredCards 的合法范围。
                        get: { min(cardIndex, max(filteredCards.count - 1, 0)) },
                        set: { cardIndex = $0 }
                    ),
                    showsCounterfactual: $showsCounterfactual
                )
                if showsCounterfactual, let card = filteredCards.indices.contains(cardIndex) ? filteredCards[cardIndex] : nil {
                    detailPanel(card)
                }
            }
        case .timeline:
            ExperienceTimelineCanvas(nodes: learning.timeline, namespace: namespace)
        case .policy:
            PolicyTransitionTrack(tracks: learning.policyTracks)
        case .lessons:
            lessons
        case .impact:
            ImpactSummaryCard(impact: learning.impact)
        }
    }

    /// Detail expands beside the stack; the current card dims but never disappears.
    private func detailPanel(_ card: RetrospectiveCardPresentation) -> some View {
        // 详情面板只展示当前卡片已有的 counterfactual、diagnostic gaps 和 P&L，不推导新的结论。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                Text(L10n.text("Counterfactual", language: language)).akzioText(.sectionTitle)
            Text(card.counterfactual.isEmpty ? MissingValue.unavailable.rawValue : card.counterfactual)
                .akzioText(.bodySmall)
            HairlineDivider()
                    Text(L10n.text("Diagnostic Gaps", language: language)).akzioText(.caption)
            if card.diagnosticGaps.isEmpty {
                        Text(L10n.text("None recorded", language: language)).akzioText(.bodySmall)
            } else {
                ForEach(card.diagnosticGaps, id: \.self) { gap in
                    Text(gap).akzioText(.bodySmall, color: AkzioColor.actionCoral)
                }
            }
            HairlineDivider()
            HStack(spacing: AkzioLayout.s2) {
                    Text(L10n.text("P&L Impact", language: language)).akzioText(.caption)
                Spacer(minLength: 4)
                Text(PpmFormatter.currency(micros: card.pnlMicros, signed: true))
                    .akzioMono(11, color: AkzioColor.primaryText)
            }
            Spacer(minLength: 0)
        }
        .padding(AkzioLayout.s4)
        .frame(width: 288, alignment: .leading)
        .akzioGlass(.elevated)
        .transition(.move(edge: .trailing).combined(with: .opacity))
    }

    private var lessons: some View {
        // Lesson 候选直接来自 sealed retrospective；空集合显示 waiting，非空列表按原投影顺序逐项 reveal。
        SectionCard(title: "Lessons", subtitle: "Promoted from sealed retrospectives") {
            if learning.lessonCandidates.isEmpty {
                StatusExplanation(.waiting, detail: "No lesson candidates for this window")
            } else {
                VStack(alignment: .leading, spacing: AkzioLayout.s3) {
                    ForEach(Array(learning.lessonCandidates.enumerated()), id: \.element.id) { index, card in
                        VStack(alignment: .leading, spacing: 3) {
                            HStack(spacing: AkzioLayout.s2) {
                                Image(systemName: "lightbulb")
                                    .font(.system(size: 10, weight: .medium))
                                    .foregroundStyle(AkzioColor.primaryGold)
                                Text(card.lessonCandidate).akzioText(.body, color: AkzioColor.primaryText)
                                Spacer(minLength: AkzioLayout.s2)
                                PillTag(card.conclusion.displayName, tone: card.conclusion.tone)
                            }
                            Text("\(L10n.text("From", language: language)) “\(card.title)” · \(card.dateLabel)").akzioText(.caption)
                        }
                        .staggeredReveal(index: index)
                    }
                }
            }
        }
    }
}
