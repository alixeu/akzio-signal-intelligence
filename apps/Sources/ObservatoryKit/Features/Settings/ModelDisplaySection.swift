import SwiftUI

// 文件职责：编辑 Canvas 质量、标签密度和推理可视化开关，并展示当前 CanvasRenderPolicy 的实际预算。
// settings 是可绑定的 UI 选择；canvasPolicy 是 Store 根据设置/窗口状态计算出的只读预算快照。
// MARK: - Model display
//
// Render budgets for the two Canvas surfaces plus the label policy. Quality is not
// cosmetic: it caps particle and node counts, which is what keeps a long-running
// window from heating up.
struct ModelDisplaySection: View {
    // canvasPolicy 的 Sendable/Equatable 值语义让本页只展示已解析预算，不直接管理 Canvas 生命周期。
    @Binding var settings: SettingsPresentation
    let canvasPolicy: CanvasRenderPolicy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // body 的 CaseIterable map 闭包生成 segmented options；budget 行只格式化 policy 输出，不反向修改设置。
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
        // 输入标签和值文本，输出只读预算单元；数值格式化已在调用点完成，helper 不承担计算策略。
        VStack(alignment: .leading, spacing: 2) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value).akzioMono(11, color: AkzioColor.primaryText)
        }
    }
}
