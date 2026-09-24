import AppKit
import SwiftUI

// MARK: - Stage inspector
//
// Title and status change first; metrics and prose follow in a short stagger, so
// the eye lands on "what is this / how is it doing" before the detail arrives.
struct StageInspectorPanel: View {
    // inspector/node 都是已由 ObservatoryStore 组装的只读 projection；onDismiss 是可选的
    // UI 闭包，面板不会从这里重新读取 Rust Store 或 provider raw payload。
    let inspector: StageInspectorPresentation
    let node: WorkflowNodePresentation?
    let namespace: Namespace.ID?
    var width: CGFloat = AkzioLayout.inspectorWidth
    var onDismiss: (() -> Void)? = nil

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // 各 section 以 projection 中的数组是否为空决定是否显示；analysisRecords 已是
        // Observer-safe 的模型/工具/ Rust 输出记录，不等于隐藏 chain-of-thought。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            header
            HairlineDivider()
            metrics.staggeredReveal(index: 1)
            if !inspector.researchAudit.isEmpty {
                section("研究审计", index: 2) {
                    PageScroll {
                        VStack(alignment: .leading, spacing: 8) {
                            ForEach(inspector.researchAudit) { row in
                                VStack(alignment: .leading, spacing: 3) {
                                    Text(row.title).akzioText(.bodySmall, color: row.isFailure ? AkzioColor.actionCoral : AkzioColor.primaryText)
                                    Text(row.detail).akzioText(.bodySmall, color: AkzioColor.mutedText).textSelection(.enabled)
                                    if !row.references.isEmpty {
                                        Text(row.references.joined(separator: "\n")).akzioMono(9, color: AkzioColor.mutedText).textSelection(.enabled)
                                    }
                                }
                            }
                        }
                    }.frame(maxHeight: 200)
                }
            }
            section("Analysis Record", index: 2) {
                Text(L10n.text(
                    "Natural-language LLM research, tool lifecycle, and Rust-validated output. Hidden reasoning and secret-bearing arguments remain excluded.",
                    language: language
                ))
                    .akzioText(.caption, color: AkzioColor.mutedText)
                if inspector.analysisRecords.isEmpty {
                    Text(L10n.text(inspector.model == "Rust"
                         ? "Rust-owned stage; no LLM or tool record applies."
                         : "No observed analysis record is available for this node.", language: language))
                        .akzioText(.bodySmall, color: AkzioColor.mutedText)
            } else {
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: AkzioLayout.s3) {
                        ForEach(inspector.analysisRecords) { record in
                            AnalysisRecordRow(record: record)
                        }
                    }
                }
                .frame(maxHeight: 360)
                .scrollIndicators(.never)
                .scrollBounceBehavior(.basedOnSize)
            }
            }
            if !inspector.blockers.isEmpty {
                section("Hard Blockers", index: 3) {
                    VStack(alignment: .leading, spacing: 5) {
                        ForEach(inspector.blockers) { blocker in
                            HStack(spacing: 6) {
                                Image(systemName: "hand.raised")
                                    .font(.system(size: 9, weight: .medium))
                                    .foregroundStyle(AkzioColor.actionCoral)
                                Text(L10n.text(blocker.displayName, language: language))
                                    .akzioText(.bodySmall, color: AkzioColor.actionCoral)
                                Spacer(minLength: 4)
                                Text(L10n.text(blocker.gate.displayName, language: language))
                                    .akzioMono(10, color: AkzioColor.mutedText)
                            }
                        }
                    }
                }
            }
            if !inspector.warnings.isEmpty {
                section("Soft Warnings", index: 4) {
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(inspector.warnings) { warning in
                            Text(L10n.text(warning.displayName, language: language)).akzioText(.bodySmall)
                        }
                    }
                }
            }
            if !inspector.alternatives.isEmpty {
                section("Alternatives", index: 5) {
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(Array(inspector.alternatives.enumerated()), id: \.offset) { index, item in
                            HStack(spacing: 6) {
                                Text("\(index + 1)").akzioMono(10, color: AkzioColor.mutedText)
                                Text(L10n.text(item, language: language)).akzioText(.bodySmall)
                            }
                            .staggeredReveal(index: index)
                        }
                    }
                }
            }
            if !inspector.uncertainties.isEmpty {
                section("Uncertainties", index: 6) {
                    VStack(alignment: .leading, spacing: 6) {
                        ForEach(inspector.uncertainties) { item in
                            VStack(alignment: .leading, spacing: 3) {
                                HStack {
                                    Text(L10n.text(item.label, language: language)).akzioText(.bodySmall)
                                    Spacer(minLength: 4)
                                    Text(PpmFormatter.share(ppm: item.weightPpm, fractionDigits: 0))
                                        .akzioMono(10, color: AkzioColor.mutedText)
                                }
                                RatioBar(fraction: PpmFormatter.fraction(ppm: item.weightPpm), height: 4)
                            }
                        }
                    }
                }
            }
            Spacer(minLength: 0)
        }
        .padding(AkzioLayout.s4)
        .frame(width: width, alignment: .leading)
        .akzioGlass(.elevated)
        .clipped()
    }

    // MARK: Header

    private var header: some View {
        // header 先展示阶段名称和 status，再按可选 onDismiss 闭包渲染关闭按钮；Environment
        // 的 motion/language 只控制动画和本地化。
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: node?.stage.symbol ?? "circle")
                    .font(.system(size: 13, weight: .medium))
                    .foregroundStyle(AkzioColor.primaryGold)
                Text(L10n.text(inspector.stageTitle, language: language))
                    .akzioText(.title)
                    .sharedElement(.modelName, in: namespace)
                Spacer(minLength: AkzioLayout.s2)
                if let onDismiss {
                    Button(action: onDismiss) {
                        Image(systemName: "xmark")
                            .font(.system(size: 9, weight: .semibold))
                            .frame(width: 24, height: 24)
                    }
                    .buttonStyle(PressableButtonStyle())
                    .help(L10n.text("Close", language: language))
                    .accessibilityLabel(L10n.text("Close", language: language))
                }
            }
            HStack(spacing: AkzioLayout.s2) {
                StatusBadge(inspector.status)
                if let detail = inspector.status.detail {
                    Text(L10n.text(detail, language: language)).akzioText(.caption)
                }
            }
        }
        .animation(policy.resolve(Motion.panel), value: inspector.stageTitle)
    }

    // MARK: Metrics

    private var metrics: some View {
        // metrics 只格式化已有计数、延迟、token 和 confidence；PpmFormatter 不会重新计算
        // Rust 的业务指标，也不会把 nil latency/token 当成零值写回 projection。
        LazyVGrid(
            columns: [GridItem(.flexible(), alignment: .leading), GridItem(.flexible(), alignment: .leading)],
            alignment: .leading,
            spacing: AkzioLayout.s3
        ) {
            metric("Model", inspector.model)
            metric("Provider", inspector.provider)
            metric("Reasoning", inspector.reasoningMode)
            metric("Turn", "\(inspector.turn)/\(inspector.totalTurns)")
            metric("Tool Calls", PpmFormatter.count(inspector.toolCalls))
            metric("Latency", PpmFormatter.latency(millis: inspector.latencyMillis))
            metric("Input Tokens", PpmFormatter.count(inspector.inputTokens))
            metric("Output Tokens", PpmFormatter.count(inspector.outputTokens))
            metric("Confidence", PpmFormatter.share(ppm: inspector.confidencePpm))
        }
    }

    private func metric(_ label: String, _ value: String) -> some View {
        // metric 的 value 已在调用点完成单位格式化；akzioNumeric 仅影响数值展示动画。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(L10n.text(value, language: language))
                .akzioMono(11, color: AkzioColor.primaryText)
                .lineLimit(1)
                .akzioNumeric(value, policy: policy)
        }
    }

    private func section<Content: View>(
        _ title: String,
        index: Int,
        @ViewBuilder content: () -> Content
    ) -> some View {
        // @ViewBuilder content 是同步的局部构建闭包；index 只决定 staggeredReveal 的动画
        // 顺序，不改变 section 的数据生命周期。
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            Text(L10n.text(title, language: language)).akzioText(.caption)
            content()
        }
        .staggeredReveal(index: index)
    }

}

// AnalysisRecordRow 只展示 Store 投影的时间线记录；ObservedMarkdown 负责本地文本分块，
// copy/timestamp 两个 helper 也只触及桌面端展示环境。
/// Chat-style Observer transcript. Model summaries conclusions share one
/// AI voice; tool lifecycle stays visually distinct.
struct AnalysisRecordRow: View {
    let record: AnalysisRecordPresentation
    var showsActor = false

    @Environment(\.appLanguage) private var language

    var body: some View {
        // tool、Rust output 和普通模型记录用不同 accent 区分；record.isStreaming 只显示
        // 活跃标记，不能据此断言底层模型连接仍然存在。
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            HStack(alignment: .firstTextBaseline, spacing: AkzioLayout.s2) {
                Text(L10n.text(record.kind.displayName, language: language))
                    .akzioMono(11, color: accent)
                if record.isStreaming {
                    Circle()
                        .fill(accent)
                        .frame(width: 5, height: 5)
                }
                Spacer(minLength: AkzioLayout.s2)
                if let createdAt = record.createdAt {
                    Text(timestamp(createdAt))
                    .akzioText(.caption, color: AkzioColor.mutedText)
                    .lineLimit(1)
                }
                if record.createdAt == nil { Text("时间未知").akzioText(.caption) }
                Button(action: copyBody) {
                    Image(systemName: "doc.on.doc")
                        .font(.system(size: 11, weight: .medium))
                        .foregroundStyle(AkzioColor.mutedText)
                        .frame(width: 24, height: 24)
                }
                .buttonStyle(PressableButtonStyle(scale: 0.94))
                .help(L10n.text("Copy", language: language))
                .accessibilityLabel(L10n.text("Copy", language: language))
            }

            if showsActor || record.taskID != nil {
                Text("\(L10n.text(record.actor, language: language)) · \(record.horizon?.uppercased() ?? "期限未知") · \(record.taskID ?? "任务未知")")
                    .akzioMono(11, color: AkzioColor.secondaryText)
                    .textSelection(.enabled)
            }
            Text(L10n.text(record.title, language: language)).akzioText(.bodySmall, color: AkzioColor.secondaryText)
            ObservedMarkdown(source: localizedBody)
                .textSelection(.enabled)
                .akzioText(.body, color: AkzioColor.primaryText)
                .lineLimit(nil)
                .multilineTextAlignment(.leading)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(AkzioLayout.s4)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background {
            ZStack {
                AkzioColor.raisedSurface.opacity(0.50)
                accent.opacity(record.kind == .tool ? 0.025 : 0.075)
            }
        }
        .overlay {
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(accent.opacity(record.kind == .tool ? 0.22 : 0.48), lineWidth: 1)
        }
        .clipShape(RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous))
    }

    private var accent: Color {
        // accent 是 kind 到颜色的确定性映射，颜色变化不参与状态判定。
        switch record.kind {
        case .tool:
            Color(nsColor: .systemPurple)
        case .rustOutput:
            Color(nsColor: .systemOrange)
        default:
            Color(nsColor: .systemBlue)
        }
    }

    private var localizedBody: String {
        // tool 记录只把第一个 " · " 前的事件名本地化，参数/其余审计文本原样保留；其他
        // 记录整体交给 L10n，避免修改 Store 中的原始 body。
        guard record.kind == .tool else {
            return L10n.text(record.body, language: language)
        }
        var parts = record.body.components(separatedBy: " · ")
        if let first = parts.first {
            parts[0] = L10n.text(first, language: language)
        }
        return parts.joined(separator: " · ")
    }

    private func copyBody() {
        // 复制写入 macOS pasteboard，不回写 record，也不触发 Core/Store 请求。
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(record.body, forType: .string)
    }

    private func timestamp(_ date: Date) -> String {
        // DateFormatter 每次按当前语言 locale 格式化观察时间；它只改变显示格式，不改变
        // projection 中的 Date 或事件顺序。
        let formatter = DateFormatter()
        formatter.locale = language.locale
        formatter.dateFormat = "dd/MM/yyyy, h:mm:ss a"
        return formatter.string(from: date)
    }
}
