import SwiftUI

// 文件职责：把 ObservatoryStore 的 Core 状态、credential draft 和 model routing 映射成设置表单。
// @Bindable 让控件直接编辑 Store 的 draft；保存/重启/清除等闭包才触发显式 Store action，展示状态仍由 Store 读取。
struct CoreSettingsSection: View {
    // store 是 @MainActor Observable 的可绑定引用；View 读取状态并通过 Binding 编辑 draft，不复制 Core 真相。
    @Bindable var store: ObservatoryStore
    @Environment(\.appLanguage) private var language

    var body: some View {
        // body 按 section 分组展示 Core/凭证/路由；Button 的 Task 闭包调用 Store 异步 action，UI 不直接写文件或 Store。
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
            footnote: "配置位置：\(store.coreConfigurationPath) · 仅当前用户可访问"
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
            "Optional Source Metadata",
            footnote: "SEC metadata is not required by the current Paper evidence policy."
        ) {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                TextField(L10n.text("SEC user agent", language: language), text: $store.coreConfigurationDraft.secUserAgent)
                    .textFieldStyle(.roundedBorder)
                }
            }

            HStack(spacing: AkzioLayout.s2) {
                // Task 闭包把显式用户动作交给 MainActor Store；Section 不直接创建或管理 Core 进程。
                Button("连接真实数据") { Task { await store.reconnectCore() } }
                    .buttonStyle(PressableButtonStyle())
                // 保存按钮的 Task 闭包执行 Store 的持久化/重启流程，draft 只有在这里才离开编辑态。
                Button(L10n.text("Save & Restart Core", language: language)) {
                    Task { await store.saveCoreConfigurationAndRestart() }
                }
                .buttonStyle(PressableButtonStyle())

                    // 清除按钮是同步 Store action；destructive role 只影响 SwiftUI 语义和样式。
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

    // credentialFields 输出绑定到同一 coreConfigurationDraft 的 credential 表单；configured 只来自已知状态。
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
            configuredField(
                "FRED API key",
                text: $store.coreConfigurationDraft.fredAPIKey,
                configured: store.coreCredentialStatus.fredAPIKey
            )
        }
    }

    // modelRoutingFields 输出全局 fallback 和每个 CoreModelStage 的可编辑路由；ForEach 通过 Binding helper 连接 stage key。
    private var modelRoutingFields: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            TextField(
                L10n.text("Provider", language: language),
                text: $store.coreConfigurationDraft.provider
            )
            .textFieldStyle(.roundedBorder)
            .help("Current production provider: openai_responses")

            modelRouteRow(
                title: "Default",
                help: "Fallback for model-mediated stages without an explicit route.",
                model: $store.coreConfigurationDraft.globalModel,
                reasoning: $store.coreConfigurationDraft.globalReasoningEffort,
                language: $store.coreConfigurationDraft.globalResponseLanguage
            )

            HairlineDivider()

            ForEach(CoreModelStage.allCases) { stage in
                // ForEach 闭包借用 stage 值；每个 row 的 get/set Binding 都以该 stage 作为稳定键。
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
        // 输入标题/help 和三个 Binding，输出单行路由编辑器；该 helper 不拥有任何路由值。
        HStack(spacing: AkzioLayout.s2) {
            Text(L10n.text(title, language: self.language))
                .akzioText(.bodySmall)
                .frame(width: 112, alignment: .leading)
                .help(L10n.text(help, language: self.language))
            TextField(L10n.text("Model", language: self.language), text: model)
                .textFieldStyle(.roundedBorder)
            Picker(L10n.text("Reasoning", language: self.language), selection: reasoning) {
                ForEach(Self.reasoningEfforts, id: \.self) { effort in
                    // Picker 闭包把固定允许值映射为 tag；选择结果由传入 reasoning Binding 回写。
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
        // 输出一个双向 String Binding：get 读取 stage override 或 global fallback，set 只更新 draft 的该 stage route。
        Binding(
            get: {
                // get 闭包只读当前 draft；缺少 stage 配置时回退全局 model，不写入默认值。
                store.coreConfigurationDraft.stageModels[stage]?.model
                    ?? store.coreConfigurationDraft.globalModel
            },
            set: { value in
                // set 闭包先复制/创建值语义 route，再替换 model 并写回 stageModels；持久化留给显式 Save action。
                var route = stageConfiguration(for: stage)
                route.model = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func reasoningBinding(for stage: CoreModelStage) -> Binding<String> {
        // 输出 reasoning effort 的双向 Binding；读路径支持全局回退，写路径只改变当前 draft。
        Binding(
            get: {
                // get 闭包不创建副作用，只选择 stage override 或 global reasoning。
                store.coreConfigurationDraft.stageModels[stage]?.reasoningEffort
                    ?? store.coreConfigurationDraft.globalReasoningEffort
            },
            set: { value in
                // set 闭包基于值语义 route 修改 reasoningEffort，再存回 draft 的 stage key。
                var route = stageConfiguration(for: stage)
                route.reasoningEffort = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func languageBinding(for stage: CoreModelStage) -> Binding<String> {
        // 输出 response language 的双向 Binding；nil/缺少 override 时显示全局语言设置。
        Binding(
            get: {
                // get 闭包只读取 stage 或 global responseLanguage，不将 fallback 固化到 Store。
                store.coreConfigurationDraft.stageModels[stage]?.responseLanguage
                    ?? store.coreConfigurationDraft.globalResponseLanguage
            },
            set: { value in
                // set 闭包只更新当前 stage 的 responseLanguage，保存仍由外层按钮显式触发。
                var route = stageConfiguration(for: stage)
                route.responseLanguage = value
                store.coreConfigurationDraft.stageModels[stage] = route
            }
        )
    }

    private func stageConfiguration(for stage: CoreModelStage) -> CoreStageModelConfiguration {
        // 输入 stage，输出已有值语义配置或由 global draft 组成的临时 fallback；不写回 stageModels。
        store.coreConfigurationDraft.stageModels[stage]
        ?? CoreStageModelConfiguration(
            model: store.coreConfigurationDraft.globalModel,
            reasoningEffort: store.coreConfigurationDraft.globalReasoningEffort,
            responseLanguage: nil
        )
    }

    // reasoningEfforts 是 UI 允许的有限字符串集合；Picker 的 tag 与 draft 字段保持同一原始值。
    private static let reasoningEfforts = [
        "none", "minimal", "low", "medium", "high", "xhigh", "max",
    ]

    private func configuredField(
        _ title: String,
        text: Binding<String>,
        configured: Bool
    ) -> some View {
        // 输入标题、credential Binding 和 configured 标记，输出 SecureField + 状态 badge；不会在这里读取明文凭证。
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
        // 输入已解析的状态文案和 AkzioStatus，输出只读状态行；状态 badge 负责统一 tone/symbol/label。
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
        // computed property 将 RustCoreState 映射为 UI 状态；它只读 store，不触发启动/停止动作。
        switch store.coreState {
        case .ready: .completed
        case .starting, .waitingReady: .running
        case .needsConfiguration: .unavailable
        case .failed: .failed
        case .stopping, .stopped: .waiting
        }
    }

    private var observerStatus: AkzioStatus {
        // computed property 将 ObserverConnectionState 映射为显示状态；mock 明确标为 notApplicable 而非成功连接。
        switch store.observerState {
        case .mock: .notApplicable
        case .connecting: .running
        case .connected: .completed
        case .stale: .stale
        case .offline: .failed
        }
    }

    private var approvalStatus: AkzioStatus {
        // computed property 将 Core approval 原始字符串映射为有限状态；未知值保守显示 unavailable。
        switch store.coreApprovalStatus {
        case "valid": .completed
        case "expired", "mismatched": .stale
        case "missing": .waiting
        default: .unavailable
        }
    }

    private var approvalCommand: String {
        // computed property 只生成供复制/展示的 CLI 模板，不执行命令，也不写入审批。
        "akzio-core --config <bundled-config> store approve-paper <SESSION> "
            + "--operator <NAME> --reason <REASON> --max-notional-usd-cents <CENTS>"
    }
}
