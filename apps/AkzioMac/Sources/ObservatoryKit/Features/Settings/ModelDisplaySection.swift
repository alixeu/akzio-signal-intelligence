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

// MARK: - Scenario switching

/// The 20 deterministic fixtures. Switching rebuilds the snapshot from the frozen
/// anchor, so the same row always produces the same screen.
struct ScenarioSection: View {
    let current: MockScenario
    let onSelect: (MockScenario) -> Void

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        SettingsSection(
            "Mock Data Scenarios",
            footnote: "Display-only fixtures built from a frozen clock. No process is started and no data is fetched."
        ) {
            VStack(spacing: 3) {
                ForEach(MockScenario.allCases) { scenario in
                    Button { onSelect(scenario) } label: {
                        HStack(spacing: AkzioLayout.s2) {
                            Text(scenario.code).akzioMono(10, color: AkzioColor.mutedText).frame(width: 22, alignment: .leading)
                            Text(scenario.title).akzioText(.bodySmall).lineLimit(1)
                            Spacer(minLength: AkzioLayout.s2)
                            if scenario == current {
                                Image(systemName: "checkmark")
                                    .font(.system(size: 9, weight: .bold))
                                    .foregroundStyle(AkzioColor.primaryGold)
                            }
                        }
                        .padding(.horizontal, AkzioLayout.s2)
                        .frame(height: 24)
                        .background {
                            if scenario == current {
                                RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                                    .fill(AkzioColor.goldFill)
                            }
                        }
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .rowHoverHighlight(isSelected: scenario == current)
                    .accessibilityAddTraits(scenario == current ? [.isSelected] : [])
                }
            }
            .animation(policy.resolve(Motion.highlight), value: current)
        }
    }
}
