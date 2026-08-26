import SwiftUI

// MARK: - Run preview
//
// Selecting a row slides this panel in from the right. It is a preview, not the
// detail page: enough to decide whether to open the run, never a second copy of
// the workflow view. The run identifier and status badge are the shared elements
// that carry over when "View Details" hands off to the Workflow page.
struct RunPreviewPanel: View {
    let row: ArchiveRowPresentation
    let stageProgress: [ArchiveStageProgress]
    let onViewDetails: () -> Void
    let onOpenInNewWindow: () -> Void
    let onDismiss: () -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            header
            HairlineDivider()
            summary.staggeredReveal(index: 1)
            stageSection.staggeredReveal(index: 2)
            resultSection.staggeredReveal(index: 3)
            Spacer(minLength: 0)
            actions.staggeredReveal(index: 4)
        }
        .padding(AkzioLayout.s4)
        .frame(width: AkzioLayout.inspectorWidth, alignment: .leading)
        .frame(maxHeight: .infinity, alignment: .top)
        .akzioGlass(.elevated)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("\(L10n.text("Run Preview", language: language)) \(row.runID.prefix(8))")
    }

    // MARK: Header

    private var header: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(alignment: .top, spacing: AkzioLayout.s2) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(L10n.text("Run", language: language)).akzioText(.caption)
                    Text(String(row.runID.prefix(8)))
                        .akzioMono(15, color: AkzioColor.primaryText)

                }
                Spacer(minLength: AkzioLayout.s2)
                Button(action: onDismiss) {
                    Image(systemName: "xmark")
                        .font(.system(size: 9, weight: .bold))
                        .foregroundStyle(AkzioColor.mutedText)
                        .frame(width: 20, height: 20)
                        .contentShape(Rectangle())
                }
                .buttonStyle(PressableButtonStyle())
            .accessibilityLabel(L10n.text("Close preview", language: language))
            }
            HStack(spacing: AkzioLayout.s2) {
                StatusBadge(row.status.status)

                PillTag(row.purposeLabel, tone: row.purpose.tone)
            }
            Text(row.runID).akzioMono(9, color: AkzioColor.mutedText).lineLimit(1)
        }
        .animation(policy.resolve(Motion.panel), value: row.id)
    }

    // MARK: Summary

    private var summary: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            Text(L10n.text("Summary", language: language)).akzioText(.caption)
            LazyVGrid(
                columns: [GridItem(.flexible(), alignment: .leading), GridItem(.flexible(), alignment: .leading)],
                alignment: .leading,
                spacing: AkzioLayout.s3
            ) {
                field("Topology", row.topology)
                field("Current Stage", row.currentStage)
                field("Model", row.model)
                field("Started", row.startedAtLabel)
                field("Duration", PpmFormatter.duration(seconds: row.durationSeconds))
                field("Status", row.status.displayName)
            }
        }
    }

    private func field(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(L10n.text(value, language: language))
                .akzioMono(11, color: AkzioColor.primaryText)
                .lineLimit(1)
                .akzioNumeric(value, policy: policy)
        }
    }

    // MARK: Stage progress

    private var stageSection: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Text(L10n.text("Stage Progress", language: language)).akzioText(.caption)
                Spacer(minLength: 4)
                Text("\(settledStages)/\(stageProgress.count)")
                    .akzioMono(10, color: AkzioColor.mutedText)
            }
            SegmentedProgressBar(completed: settledStages, total: max(stageProgress.count, 1))
            VStack(alignment: .leading, spacing: 5) {
                ForEach(Array(stageProgress.enumerated()), id: \.element.id) { index, stage in
                    HStack(spacing: 6) {
                        StatusDot(stage.status, diameter: 6)
                        Text(L10n.text(stage.label, language: language)).akzioText(.bodySmall).lineLimit(1)
                        Spacer(minLength: 4)
                        Text(L10n.text(stage.timeLabel, language: language)).akzioMono(10, color: AkzioColor.mutedText)
                    }
                    .staggeredReveal(index: index, travel: 6)
                }
            }
        }
    }

    /// Stages that have reached a terminal reading. Running/queued/observing work is
    /// deliberately excluded so the counter never over-reports progress.
    private var settledStages: Int {
        stageProgress.filter { stage in
            switch stage.status {
            case .succeeded, .completed, .completedWithRejection, .accepted, .rejected,
                 .failed, .blocked, .cancelled, .skipped, .notTriggered, .notApplicable:
                true
            case .running, .leased, .queued, .observing, .waiting, .partial, .unavailable, .stale:
                false
            }
        }.count
    }

    // MARK: Result

    private var resultSection: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            Text(L10n.text("Result", language: language)).akzioText(.caption)
            HStack(alignment: .firstTextBaseline, spacing: AkzioLayout.s2) {
                if let result = row.resultPpm {
                    Text(PpmFormatter.percent(ppm: result))
                        .akzioMono(22, color: result >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral)
                        .akzioNumeric(Double(result), policy: policy)
                } else {
                    // No sealed number yet — the run has no result to show, and a zero
                    // here would read as a flat outcome that never happened.
                    UnavailableValue(row.status.resultMissingValue, size: 15)
                }
                Spacer(minLength: 4)
            }
            Text(L10n.text(row.status.resultCaption, language: language))
                .akzioText(.bodySmall, color: AkzioColor.secondaryText)
                .fixedSize(horizontal: false, vertical: true)
        }
        .animation(policy.resolve(Motion.numeric), value: row.resultPpm ?? Int.min)
    }

    // MARK: Actions

    private var actions: some View {
        VStack(spacing: AkzioLayout.s2) {
            Button(action: onViewDetails) {
                HStack(spacing: 5) {
                    Image(systemName: "arrow.right.circle").font(.system(size: 10, weight: .semibold))
                    Text(L10n.text("View Details", language: language)).akzioText(.label, color: AkzioColor.deepBackground)
                }
                .frame(maxWidth: .infinity)
                .frame(height: 28)
                .background(
                    RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                        .fill(AkzioColor.primaryGold)
                )
                .foregroundStyle(AkzioColor.deepBackground)
            }
            .buttonStyle(PressableButtonStyle())
            Button(action: onOpenInNewWindow) {
                HStack(spacing: 5) {
                    Image(systemName: "macwindow.badge.plus").font(.system(size: 10, weight: .medium))
                Text(L10n.text("Open in New Window", language: language)).akzioText(.label)
                }
                .frame(maxWidth: .infinity)
                .frame(height: 28)
                .akzioGlassBackdrop(AkzioColor.deepBackground, radius: AkzioLayout.chipRadius)
                .overlay(
                    RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                        .strokeBorder(AkzioColor.hairline, lineWidth: 1)
                )
            }
            .buttonStyle(PressableButtonStyle())
        }
    }
}

// MARK: - Result semantics

extension WorkflowStatus {
    /// Why a run has no result number. A decision that finished without an order is
    /// Not Applicable, not a failure — there is nothing for a horizon to measure.
    var resultMissingValue: MissingValue {
        switch self {
        case .queued, .leased, .running: .pending
        case .decisionCompleted: .waiting
        case .completed, .completedWithExecutionRejection: .unavailable
        case .failed, .cancelled: .notApplicable
        }
    }

    var resultCaption: String {
        switch self {
        case .queued: "Not started — no result exists yet."
        case .leased: "Leased by a worker; execution has not begun."
        case .running: "Result is sealed after the outcome horizon closes."
        case .decisionCompleted: "Decision sealed. Awaiting the outcome horizon."
        case .completed: "Sealed outcome for the run's canonical horizon."
        case .completedWithExecutionRejection: "Sealed with an execution rejection on record."
        case .failed: "Run ended in error; no outcome was sealed."
        case .cancelled: "Cancelled before the horizon could seal."
        }
    }
}
