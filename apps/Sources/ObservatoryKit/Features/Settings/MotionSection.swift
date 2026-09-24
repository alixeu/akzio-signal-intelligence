import SwiftUI

// 文件职责：编辑全局动效、强度、粒子密度和页面转场，并用 resolved MotionPolicy 做实时预览。
// settings 是父级值语义设置；resolvedPolicy 是 Store 已解析的只读策略，policy Environment 控制过渡动画本身。
// MARK: - Motion
//
// Turning Global Motion off is not the same as Reduce Motion: this kills ambient
// loops and page travel, while Reduce Motion additionally collapses transitions to
// a short crossfade. Both are shown here so the difference is visible.
struct MotionSection: View {
    // Binding 负责把控件值写回 SettingsPresentation；resolvedPolicy 描述系统限制与用户设置合并后的结果。
    @Binding var settings: SettingsPresentation
    let resolvedPolicy: MotionPolicy

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        // body 只在 globalMotionEnabled 为真时启用相关滑块；预览文案直接反映 resolvedPolicy，而非自行推断系统状态。
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
    // resolvedPolicy 是不可变值语义策略；strip 只演示允许的位移，不改变策略或设置。
    let resolvedPolicy: MotionPolicy

    var body: some View {
        // ForEach 闭包按固定三个索引创建 dot；每个 dot 从同一 policy 计算 travel。
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
        // 输入索引和 resolvedPolicy，输出静态或 phaseAnimator View；index 只用于动画 stagger，不是业务 ID。
        let travel = resolvedPolicy.travel(22)
        if resolvedPolicy.allowsAmbient {
            Circle()
                .fill(AkzioColor.primaryGold.opacity(0.9))
                .frame(width: 8, height: 8)
                .phaseAnimator([false, true]) { view, phase in
                    // phase 闭包把布尔动画相位映射成位移/透明度；不写回 settings。
                    view
                        .offset(x: phase ? travel : 0)
                        .opacity(phase ? 1 : 0.55)
                } animation: { _ in
                    // animation 闭包根据 dot index 生成固定 delay，保持三个预览点的顺序。
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
