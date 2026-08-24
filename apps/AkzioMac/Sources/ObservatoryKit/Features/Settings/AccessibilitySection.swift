import SwiftUI

// MARK: - Accessibility
//
// These are overrides layered on top of the system settings, never replacements:
// if macOS asks for Reduce Motion the app honours it whether or not the toggle
// here is on. The rows show the system value so the two are never confused.
struct AccessibilitySection: View {
    @Binding var settings: SettingsPresentation
    let systemReduceMotion: Bool
    let systemReduceTransparency: Bool
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection(
                "Motion and Materials",
                footnote: "System settings win: an override can only add restrictions, never remove them."
            ) {
                SettingsToggle(
                    "Reduce Motion",
                    detail: systemStatus(systemReduceMotion),
                    isOn: $settings.reduceMotionOverride
                )
                SettingsToggle(
                    "Reduce Transparency",
                    detail: systemStatus(systemReduceTransparency),
                    isOn: $settings.reduceTransparencyOverride
                )
                SettingsToggle(
                    "High Contrast",
                    detail: "Near-solid surfaces and explicit borders.",
                    isOn: $settings.highContrast
                )
            }
            HairlineDivider()
            SettingsSection("Reading") {
                SettingsSlider(
                    "Text Size",
                    detail: "Scales body and label text; monospaced numerals stay aligned.",
                    value: $settings.textScale,
                    range: 0.9...1.3,
                    format: { PpmFormatter.multiple(ppm: Int($0 * PpmFormatter.ppmPerUnit)) }
                )
                SettingsToggle(
                    "Color-Independent Status",
                    detail: "Every status also carries a glyph and a word.",
                    isOn: $settings.colorIndependentStatus
                )
            }
            HairlineDivider()
            SettingsSection("Navigation") {
                SettingsToggle(
                    "Keyboard Focus Ring",
                    detail: "Gold focus ring on the focused control.",
                    isOn: $settings.keyboardFocusVisible
                )
            HStack {
                    Text(L10n.text("VoiceOver Labels", language: language)).akzioText(.body)
                Spacer()
                    PillTag(L10n.text("Always On", language: language), tone: .gold)
            }
            }
        }
    }

    private func systemStatus(_ enabled: Bool) -> String {
        enabled ? "System: on — already applied" : "System: off"
    }
}

// MARK: - Environment

/// Read-only build facts. Nothing here is a control, so nothing here animates.
struct EnvironmentInfoSection: View {
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection("Build") {
                VStack(spacing: 0) {
                    ForEach(SettingsPresentation.environmentRows, id: \.0) { row in
                        HStack(spacing: AkzioLayout.s2) {
                            Image(systemName: row.2)
                                .font(.system(size: 11))
                                .foregroundStyle(AkzioColor.mutedText)
                                .frame(width: 16)
                            Text(L10n.text(row.0, language: language)).akzioText(.body)
                            Spacer(minLength: AkzioLayout.s2)
                            Text(row.1).akzioMono(11, color: AkzioColor.primaryText)
                        }
                        .frame(height: 26)
                    }
                }
            }
        }
    }
}
