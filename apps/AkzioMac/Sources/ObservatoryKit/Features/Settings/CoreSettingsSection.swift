import SwiftUI

struct CoreSettingsSection: View {
    @Bindable var store: ObservatoryStore
    @Environment(\.appLanguage) private var language

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s4) {
            SettingsSection(
                "Rust Core",
                footnote: "The bundled Paper-only daemon starts with the App and stops on Quit."
            ) {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                statusRow(
                    "Core",
                    store.coreState.detail ?? store.coreState.label,
                    coreStatus
                )
                    statusRow("Observer", store.observerState.label, observerStatus)
                    statusRow("Paper Approval", store.coreApprovalStatus.capitalized, approvalStatus)
                    statusRow(
                        "Store",
                        store.coreStorePath.isEmpty
                            ? MissingValue.unavailable.rawValue
                            : store.coreStorePath,
                        store.coreStorePath.isEmpty ? .unavailable : .completed
                    )
                }
            }

            HairlineDivider()

            SettingsSection(
                "Required Credentials",
            footnote: "Configuration is saved in ~/.akzio/config.toml with owner-only permissions."
            ) {
                credentialFields
            }

            HairlineDivider()

            SettingsSection(
                "Model Routing",
            footnote: "Defaults apply globally; each model-mediated Rust stage can override model, reasoning effort, and response language."
            ) {
                modelRoutingFields
            }

            HairlineDivider()

            SettingsSection(
                "Optional Evidence",
                footnote: "Missing optional sources do not prevent Core startup."
            ) {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    SecureField(L10n.text("FRED API key", language: language), text: $store.coreConfigurationDraft.fredAPIKey)
                        .textFieldStyle(.roundedBorder)
                    TextField(L10n.text("SEC user agent", language: language), text: $store.coreConfigurationDraft.secUserAgent)
                        .textFieldStyle(.roundedBorder)
                }
            }

            HStack(spacing: AkzioLayout.s2) {
                Button(L10n.text("Save & Restart Core", language: language)) {
                    Task { await store.saveCoreConfigurationAndRestart() }
                }
                .buttonStyle(PressableButtonStyle())

                    Button(L10n.text("Clear Credentials", language: language), role: .destructive) {
                    store.clearCoreCredentials()
                }
                .buttonStyle(PressableButtonStyle())
            }

            SettingsSection(
                "Paper Approval",
                footnote: "Approval remains a separate Rust CLI action; Observatory never writes it."
            ) {
                Text(approvalCommand)
                    .akzioMono(10, color: AkzioColor.secondaryText)
                    .textSelection(.enabled)
            }

            if !store.coreSupervisor.recentOutput.isEmpty {
                SettingsSection("Recent Core Output") {
                    Text(store.coreSupervisor.recentOutput)
                        .akzioMono(9, color: AkzioColor.mutedText)
                        .textSelection(.enabled)
                        .lineLimit(12)
                }
            }
        }
    }

    private var credentialFields: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            TextField(L10n.text("LLM Gateway base URL", language: language), text: $store.coreConfigurationDraft.llmBaseURL)
                .textFieldStyle(.roundedBorder)
            configuredField(
                "LLM API key",
                text: $store.coreConfigurationDraft.llmAPIKey,
                configured: store.coreCredentialStatus.llmAPIKey
            )
            configuredField(
                "Alpaca API key",
                text: $store.coreConfigurationDraft.alpacaAPIKey,
                configured: store.coreCredentialStatus.alpacaAPIKey
            )
            configuredField(
                "Alpaca API secret",
                text: $store.coreConfigurationDraft.alpacaAPISecret,
                configured: store.coreCredentialStatus.alpacaAPISecret
            )
        }
    }

    private var modelRoutingFields: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            modelRouteRow(
                title: "Default",
                help: "Fallback for model-mediated stages without an explicit route.",
                model: $store.coreConfigurationDraft.globalModel,
                reasoning: $store.coreConfigurationDraft.globalReasoningEffort,
                language: $store.coreConfigurationDraft.globalResponseLanguage
            )

            HairlineDivider()

            ForEach(CoreModelStage.allCases) { stage in
                modelRouteRow(
                    title: stage.displayName,
                    help: stage.rawValue,
                    model: modelBinding(for: stage),
                    reasoning: reasoningBinding(for: stage),
                    language: languageBinding(for: stage)
                )
            }
        }
    }

    private func modelRouteRow(
        title: String,
        help: String,
        model: Binding<String>,
        reasoning: Binding<String>,
        language: Binding<String>
    ) -> some View {
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(title, language: self.language))
                .akzioText(.bodySmall)
                .frame(width: 112, alignment: .leading)
                .help(L10n.text(help, language: self.language))
            TextField(L10n.text("Model", language: self.language), text: model)
                .textFieldStyle(.roundedBorder)
            Picker(L10n.text("Reasoning", language: self.language), selection: reasoning) {
                ForEach(Self.reasoningEfforts, id: \.self) { effort in
                    Text(L10n.text(effort.capitalized, language: self.language)).tag(effort)
                }
            }
            .labelsHidden()
            .pickerStyle(.menu)
            .frame(width: 112)
            TextField(L10n.text("Response Language", language: self.language), text: language)
                .textFieldStyle(.roundedBorder)
                .frame(width: 112)
        }
    }

    private func modelBinding(for stage: CoreModelStage) -> Binding<String> {
        Binding(
            get: {
                store.coreConfigurationDraft.stageModels[stage]?.model
                    ?? store.coreConfigurationDraft.globalModel
            },
            set: { value in
                var route = stageConfiguration(for: stage)
                route.model = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func reasoningBinding(for stage: CoreModelStage) -> Binding<String> {
        Binding(
            get: {
                store.coreConfigurationDraft.stageModels[stage]?.reasoningEffort
                    ?? store.coreConfigurationDraft.globalReasoningEffort
            },
            set: { value in
                var route = stageConfiguration(for: stage)
                route.reasoningEffort = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func languageBinding(for stage: CoreModelStage) -> Binding<String> {
        Binding(
            get: {
                store.coreConfigurationDraft.stageModels[stage]?.responseLanguage
                    ?? store.coreConfigurationDraft.globalResponseLanguage
            },
            set: { value in
                var route = stageConfiguration(for: stage)
                route.responseLanguage = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func stageConfiguration(for stage: CoreModelStage) -> CoreStageModelConfiguration {
        store.coreConfigurationDraft.stageModels[stage]
        ?? CoreStageModelConfiguration(
            model: store.coreConfigurationDraft.globalModel,
            reasoningEffort: store.coreConfigurationDraft.globalReasoningEffort,
            responseLanguage: nil
        )
    }

    private static let reasoningEfforts = [
        "none", "minimal", "low", "medium", "high", "xhigh", "max",
    ]

    private func configuredField(
        _ title: String,
        text: Binding<String>,
        configured: Bool
    ) -> some View {
        HStack(spacing: AkzioLayout.s2) {
            SecureField(
                configured
                    ? "\(L10n.text(title, language: language)) — \(L10n.text("configured; enter to replace", language: language))"
                    : L10n.text(title, language: language),
                text: text
            )
                .textFieldStyle(.roundedBorder)
            StatusBadge(configured ? .completed : .unavailable)
        }
    }

    private func statusRow(_ label: String, _ value: String, _ status: AkzioStatus) -> some View {
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(label, language: language)).akzioText(.bodySmall)
            Spacer(minLength: AkzioLayout.s2)
            Text(L10n.text(value, language: language))
                .akzioMono(10, color: AkzioColor.secondaryText)
                .lineLimit(1)
                .help(L10n.text(value, language: language))
            StatusBadge(status)
        }
    }

    private var coreStatus: AkzioStatus {
        switch store.coreState {
        case .ready: .completed
        case .starting, .waitingReady: .running
        case .needsConfiguration: .unavailable
        case .failed: .failed
        case .stopping, .stopped: .waiting
        }
    }

    private var observerStatus: AkzioStatus {
        switch store.observerState {
        case .mock: .notApplicable
        case .connecting: .running
        case .connected: .completed
        case .stale: .stale
        case .offline: .failed
        }
    }

    private var approvalStatus: AkzioStatus {
        switch store.coreApprovalStatus {
        case "valid": .completed
        case "expired", "mismatched": .stale
        case "missing": .waiting
        default: .unavailable
        }
    }

    private var approvalCommand: String {
        "akzio-core --config <bundled-config> store approve-paper <SESSION> "
            + "--operator <NAME> --reason <REASON> --max-notional-usd-cents <CENTS>"
    }
}
