import SwiftUI

// MARK: - Model display
//
// Render budgets for the two Canvas surfaces plus the label policy. Quality is not
// cosmetic: it caps particle and node counts, which is what keeps a long-running
// window from heating up.
struct ModelDisplaySection: View {
    @Binding var settings: SettingsPresentation
    let canvasPolicy: CanvasRenderPolicy
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection("Rendering") {
                SettingsSegmented(
                    "Render Quality",
                    detail: "Caps particles, nodes and frame interval.",
                    selection: $settings.renderQuality,
                    options: CanvasRenderPolicy.Quality.allCases.map { (value: $0, label: $0.displayName) }
                )
                SettingsSegmented(
                    "Label Density",
                    detail: "Auto hides labels when nodes crowd.",
                    selection: $settings.labelDensity,
                    options: SettingsPresentation.LabelDensity.allCases.map { (value: $0, label: $0.displayName) }
                )
                SettingsToggle(
                    "Reasoning Visualization",
                    detail: "Draws the intensity orbit around active agents.",
                    isOn: $settings.showsReasoningVisualization
                )
            }
            HairlineDivider()
            SettingsSection("Active Budget") {
                LazyVGrid(
                    columns: [GridItem(.flexible(), alignment: .leading), GridItem(.flexible(), alignment: .leading)],
                    alignment: .leading,
                    spacing: AkzioLayout.s3
                ) {
                    budget("Universe Particles", PpmFormatter.count(canvasPolicy.particleBudget))
                    budget("Path Particles", PpmFormatter.count(canvasPolicy.pathParticleBudget))
                    budget("Frame Interval", "\(Int(canvasPolicy.frameInterval * 1000))ms")
                    budget(
                        "Ambient Loops",
                        L10n.text(canvasPolicy.runsAmbient ? "Running" : "Paused", language: language)
                    )
                }
            }
        }
    }

    private func budget(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value).akzioMono(11, color: AkzioColor.primaryText)
        }
    }
}
