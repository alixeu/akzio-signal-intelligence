import SwiftUI

// MARK: - Intelligence
//
// Durable agenda items on the left and the Observer-safe analysis record on the
// right. The page never invents a meeting from configured models: without observed
// trajectory or validated artifacts it renders an explicit empty state.
struct IntelligencePage: View {
    // store 是 Rust/Observer 数据投影的入口；页面只读取 displayCouncil，不在 SwiftUI 层补造分析记录。
    let store: ObservatoryStore

    // Environment 只提供当前显示语言，切换语言不会改变 council 的来源或内容。
    @Environment(\.appLanguage) private var language

    // 计算属性保持 body 的数据流单一：主题和记录都来自同一次 store 投影。
    private var council: CouncilPresentation { store.displayCouncil }

    var body: some View {
        // PageScaffold 的两个闭包分别负责主体与 toolbar；空集合明确显示缺少 Observer 证据的状态。
        PageScaffold(route: .intelligence) {
            if council.topics.isEmpty && council.analysisRecords.isEmpty {
                // 只有同时没有 topics 和 records 才进入空态，避免把部分观察误报成完全无数据。
                emptyState
            } else {
                PageScroll {
                    HStack(alignment: .top, spacing: AkzioLayout.s4) {
                        StagedSection(index: 0) { agenda }
                        StagedSection(index: 1) { analysisTimeline }
                    }
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                }
            }
        } toolbar: {
            HStack(spacing: AkzioLayout.s2) {
                PillTag(
                    "\(council.topics.count) \(L10n.text("Topics", language: language))",
                    tone: .neutral
                )
                PillTag(
                    "\(council.analysisRecords.count) \(L10n.text("Records", language: language))",
                    tone: .gold
                )
            }
        }
    }

    private var agenda: some View {
        // agenda 将持久化 topics 直接映射成可读列表；空集合只显示 unavailable 文案。
        SectionCard(
            title: "Observed Topics",
            subtitle: "Derived from durable deliberation and validated artifacts"
        ) {
            if council.topics.isEmpty {
                Text(L10n.text("No observed topics are available.", language: language))
                    .akzioText(.bodySmall, color: AkzioColor.mutedText)
            } else {
                LazyVStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    ForEach(council.topics) { topic in
                        VStack(alignment: .leading, spacing: 5) {
                            HStack(spacing: AkzioLayout.s2) {
                                PillTag(topic.kind.displayName, tone: topic.kind.tone)
                                Spacer(minLength: 0)
                                Text(L10n.text(topic.source, language: language))
                                    .akzioText(.caption, color: AkzioColor.mutedText)
                            }
                            Text(topic.title)
                                .akzioText(.bodySmall, color: AkzioColor.primaryText)
                                .fixedSize(horizontal: false, vertical: true)
                                .textSelection(.enabled)
                        }
                        .padding(AkzioLayout.s2)
                        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 8)
                    }
                }
            }
        }
        .frame(width: 360)
    }

    private var analysisTimeline: some View {
        // analysisRecords 按 store 提供的顺序展示，并把 actor 标识交给 AnalysisRecordRow。
        SectionCard(
            title: "Analysis Process",
            subtitle: "Chronological Observer record · no hidden reasoning"
        ) {
            if council.analysisRecords.isEmpty {
                Text(L10n.text("No analysis process has been observed.", language: language))
                    .akzioText(.bodySmall, color: AkzioColor.mutedText)
            } else {
                LazyVStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    ForEach(council.analysisRecords) { record in
                        AnalysisRecordRow(record: record, showsActor: true)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity)
    }

    private var emptyState: some View {
        // 空态解释数据需要 Rust 发出 Observer-safe trajectory 或 validated artifact，不从 configured model 猜测。
        SectionCard(title: "Observed Intelligence") {
            ContentUnavailableView {
                Label(
                    L10n.text("No observed analysis yet", language: language),
                    systemImage: "text.bubble"
                )
            } description: {
                Text(L10n.text(
                    "Topics and analysis records appear only after Rust emits Observer-safe trajectory or validated artifacts.",
                    language: language
                ))
            }
        }
        .frame(maxWidth: .infinity, minHeight: 360)
    }
}
