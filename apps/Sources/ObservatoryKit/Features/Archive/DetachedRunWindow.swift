import SwiftUI

public struct DetachedRunPayload: Codable, Hashable, Identifiable {
    public let id: String
    public let purpose: String
    public let topology: String
    public let status: String
    public let duration: String
    public let currentStage: String
    public let model: String
    public let result: String
    public let started: String
    public let stages: [Stage]
    public let outcomeCaption: String
    public let language: String?

    public struct Stage: Codable, Hashable, Identifiable {
        public let label: String
        public let status: String
        public let time: String
        public let id: String
    }

    init(_ row: ArchiveRowPresentation, stages progress: [ArchiveStageProgress]? = nil, outcomeEvidence: OutcomeEvidencePresentation = .unknown, language: AppLanguage? = nil) {
        id = row.runID
        purpose = row.purposeLabel
        topology = row.topology
        status = row.status.rawValue
        duration = PpmFormatter.duration(seconds: row.durationSeconds)
        currentStage = row.currentStage
        model = row.model
        result = PpmFormatter.percent(ppm: row.resultPpm)
        started = row.startedAtLabel
        self.language = language?.resolved.rawValue
        outcomeCaption = outcomeEvidence.caption
        stages = (progress ?? row.stageProgress).map {
            Stage(label: $0.displayLabel, status: $0.status.rawValue, time: $0.timeLabel, id: $0.id)
        }
    }
}

struct DetachedRunWindow: View {
    let payload: DetachedRunPayload

    @Environment(\.dismiss) private var dismiss
    @Environment(\.appLanguage) private var inheritedLanguage
    private var language: AppLanguage { payload.language.flatMap(AppLanguage.init(rawValue:)) ?? inheritedLanguage }

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(L10n.text("Run Details", language: language)).akzioText(.title)
                    Text(payload.id).akzioMono(11, color: AkzioColor.secondaryText).textSelection(.enabled)
                }
                Spacer(minLength: AkzioLayout.s3)
                StatusBadge(WorkflowStatus(rawValue: payload.status)?.status ?? .unavailable)
                Button { dismiss() } label: {
                    Image(systemName: "xmark").frame(width: 24, height: 24)
                }
                .buttonStyle(PressableButtonStyle())
            }

            LazyVGrid(
                columns: Array(repeating: GridItem(.flexible(), alignment: .leading), count: 3),
                alignment: .leading,
                spacing: AkzioLayout.s3
            ) {
                field("Purpose", payload.purpose)
                field("Topology", payload.topology)
                field("Current Stage", payload.currentStage)
                field("Started", payload.started)
                field("Duration", payload.duration)
                field("Result", payload.result)
                field("Model", payload.model)
            }

            Text(payload.outcomeCaption).akzioText(.bodySmall)
            HairlineDivider()
            Text(L10n.text("Stage Progress", language: language)).akzioText(.sectionTitle)
            PageScroll {
                VStack(spacing: 0) {
                    ForEach(payload.stages) { stage in
                        HStack(spacing: AkzioLayout.s2) {
                            StatusDot(AkzioStatus(rawValue: stage.status) ?? .unavailable, diameter: 6)
                            Text(stage.label).akzioText(.body)
                            Spacer(minLength: AkzioLayout.s3)
                            Text(L10n.text(stage.time, language: language)).akzioMono(11, color: AkzioColor.mutedText)
                        }
                        .frame(height: 28)
                        HairlineDivider()
                    }
                }
            }
        }
        .padding(AkzioLayout.s5)
        .frame(minWidth: 680, minHeight: 500)
        .akzioGlassBackdrop(AkzioColor.background(for: .dark))
        .background(WindowChromeConfigurator())
        .preferredColorScheme(.dark)
        .environment(\.appLanguage, language)
        .environment(\.locale, language.locale)
    }

    private func field(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(L10n.text(value, language: language)).akzioMono(11, color: AkzioColor.primaryText).lineLimit(1).help(value).textSelection(.enabled)
        }
    }
}
