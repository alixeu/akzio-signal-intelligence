import SwiftUI

// MARK: - Motion
//
// Turning Global Motion off is not the same as Reduce Motion: this kills ambient
// loops and page travel, while Reduce Motion additionally collapses transitions to
// a short crossfade. Both are shown here so the difference is visible.
struct MotionSection: View {
    @Binding var settings: SettingsPresentation
    let resolvedPolicy: MotionPolicy

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection("Global") {
                SettingsToggle(
                    "Global Motion",
                    detail: "Off stops ambient orbits, particles and page travel.",
                    isOn: $settings.globalMotionEnabled
                )
                SettingsSlider(
                    "Motion Intensity",
                    detail: "Scales travel distance and particle count.",
                    value: $settings.motionIntensity,
                    range: 0...1
                )
                .disabled(!settings.globalMotionEnabled)
                SettingsSlider(
                    "Particle Density",
                    detail: "Signal particles per edge on the Universe and DAG.",
                    value: $settings.particleDensity,
                    range: 0...1
                )
                .disabled(!settings.globalMotionEnabled)
                SettingsSlider(
                    "Route Transition Strength",
                    detail: "0 hands pages over with a plain crossfade.",
                    value: $settings.routeTransitionStrength,
                    range: 0...1
                )
            }
            HairlineDivider()
            SettingsSection(
                "Reduce Motion Preview",
                footnote: "Reduced motion keeps opacity and colour cues and drops travel to at most 6pt."
            ) {
                MotionPreviewStrip(resolvedPolicy: resolvedPolicy)
                HStack(spacing: AkzioLayout.s2) {
                    Chip(
                        L10n.text(resolvedPolicy.isReduced ? "Reduced" : "Full", language: language),
                        kind: .tag,
                        isSelected: resolvedPolicy.isReduced
                    )
                    Text(L10n.text(
                        resolvedPolicy.allowsAmbient ? "Ambient loops running" : "Ambient loops stopped",
                        language: language
                    ))
                        .akzioText(.caption)
                    Spacer(minLength: 0)
                    Text("\(L10n.text("Travel", language: language)) \(Int(resolvedPolicy.travel(24))) pt")
                        .akzioMono(10, color: AkzioColor.mutedText)
                }
            }
        }
        .animation(policy.resolve(Motion.control), value: settings.globalMotionEnabled)
    }
}

// MARK: - Preview strip

/// Three dots travelling the distance the current policy allows. Driven by a
/// `phaseAnimator` so it demonstrates the policy instead of describing it.
struct MotionPreviewStrip: View {
    let resolvedPolicy: MotionPolicy

    var body: some View {
        HStack(spacing: AkzioLayout.s3) {
            ForEach(0..<3, id: \.self) { index in
                dot(index: index)
            }
            Spacer(minLength: 0)
        }
        .padding(AkzioLayout.s3)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlassBackdrop(AkzioColor.deepBackground, radius: AkzioLayout.cardRadius)
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: 1)
        )
        .accessibilityHidden(true)
    }

    @ViewBuilder
    private func dot(index: Int) -> some View {
        let travel = resolvedPolicy.travel(22)
        if resolvedPolicy.allowsAmbient {
            Circle()
                .fill(AkzioColor.primaryGold.opacity(0.9))
                .frame(width: 8, height: 8)
                .phaseAnimator([false, true]) { view, phase in
                    view
                        .offset(x: phase ? travel : 0)
                        .opacity(phase ? 1 : 0.55)
                } animation: { _ in
                    .easeInOut(duration: 1.1).delay(Double(index) * 0.12)
                }
        } else {
            // Ambient motion is off: show the static end state, not a frozen frame
            // mid-animation.
            Circle()
                .fill(AkzioColor.primaryGold.opacity(0.9))
                .frame(width: 8, height: 8)
                .offset(x: travel)
        }
    }
}
