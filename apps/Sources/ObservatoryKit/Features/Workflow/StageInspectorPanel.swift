import AppKit
import SwiftUI

// MARK: - Stage inspector
//
// Title and status change first; metrics and prose follow in a short stagger, so
// the eye lands on "what is this / how is it doing" before the detail arrives.
struct StageInspectorPanel: View {
    let inspector: StageInspectorPresentation
    let node: WorkflowNodePresentation?
    let namespace: Namespace.ID?
    var width: CGFloat = AkzioLayout.inspectorWidth
    var onDismiss: (() -> Void)? = nil

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            header
            HairlineDivider()
            metrics.staggeredReveal(index: 1)
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
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            Text(L10n.text(title, language: language)).akzioText(.caption)
            content()
        }
        .staggeredReveal(index: index)
    }

}

/// Chat-style Observer transcript. Model summaries conclusions share one
/// AI voice; tool lifecycle stays visually distinct.
struct AnalysisRecordRow: View {
    let record: AnalysisRecordPresentation
    var showsActor = false

    @Environment(\.appLanguage) private var language

    var body: some View {
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

            Text(localizedBody)
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
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(localizedBody, forType: .string)
    }

    private func timestamp(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_GB")
        formatter.dateFormat = "dd/MM/yyyy, h:mm:ss a"
        return formatter.string(from: date)
    }
}
