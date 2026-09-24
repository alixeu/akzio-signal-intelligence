import SwiftUI

// MARK: - Canvas toolbar
//
// Ten controls. Zoom / pan / visibility are real view state; the rest are visual
// affordances that acknowledge the click and change nothing in the system — this
// build never writes to the Store.
struct CanvasToolbar: View {
    // 这些 Binding 由 WorkflowPage 持有；工具栏只改变画布变换和显示开关，不直接改
    // Workflow/Rust 状态，也不写 Store。
    @Binding var scale: CGFloat
    @Binding var offset: CGSize
    @Binding var showsLabels: Bool
    @Binding var showsParticles: Bool
    @Binding var showsGrid: Bool
    @Binding var highlightsCriticalPath: Bool
    @Binding var collapsesOptional: Bool

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var acknowledged: String?

    var body: some View {
        // 前四个按钮分别调整 scale/offset，五个 toggle 写回父 View 的本地 Bool；
        // acknowledged 只给点击后的普通按钮提供视觉确认，不表示 Core 已接受操作。
        HStack(spacing: AkzioLayout.s1) {
            button("minus.magnifyingglass", "Zoom Out") { zoom(by: 0.9) }
            button("plus.magnifyingglass", "Zoom In") { zoom(by: 1.1) }
            button("arrow.up.left.and.arrow.down.right", "Fit") { reset() }
            button("arrow.counterclockwise", "Reset View") { reset() }
            HairlineDivider(.vertical).frame(height: 16)
            toggle("textformat.size", "Labels", $showsLabels)
            toggle("sparkles", "Particles", $showsParticles)
            toggle("grid", "Grid", $showsGrid)
            toggle("bolt.horizontal", "Critical Path", $highlightsCriticalPath)
            toggle("arrow.down.right.and.arrow.up.left", "Collapse Optional", $collapsesOptional)
        }
        .padding(.horizontal, AkzioLayout.s2)
        .padding(.vertical, 5)
        .akzioGlass(.elevated, radius: AkzioLayout.chipRadius)
    }

    private func zoom(by factor: CGFloat) {
        // 缩放在 0.5～2.0 之间截断，动画策略来自 Environment；这里没有重新计算 DAG。
        withAnimation(policy.resolve(Motion.control)) {
            scale = min(max(scale * factor, 0.5), 2.0)
        }
    }

    private func reset() {
        // Reset 同时恢复设计比例和画布偏移，Fit 目前复用同一条本地 reset 路径。
        withAnimation(policy.resolve(Motion.panel)) {
            scale = 1
            offset = .zero
        }
    }

    private func button(_ symbol: String, _ title: String, action: @escaping () -> Void) -> some View {
        // @escaping action 在 Button 点击时执行；先保存 title 到 @State，再执行调用方闭包，
        // 因而闭包只拥有父 View 明确传入的局部 UI 操作。
        Button {
            acknowledged = title
            action()
        } label: {
            Image(systemName: symbol)
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(AkzioColor.secondaryText)
                .frame(width: 24, height: 22)
                .background {
                    if acknowledged == title {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(AkzioColor.gold(0.16))
                    }
                }
        }
        .buttonStyle(PressableButtonStyle())
        .help(L10n.text(title, language: language))
        .animation(policy.resolve(Motion.hover), value: acknowledged)
    }

    private func toggle(_ symbol: String, _ title: String, _ binding: Binding<Bool>) -> some View {
        // Binding.wrappedValue.toggle() 是同步的本地状态变更；它不会触发网络请求或 Rust
        // workflow 变更，Environment 中的 language/policy 只影响提示文字和动画。
        Button {
            withAnimation(policy.resolve(Motion.control)) { binding.wrappedValue.toggle() }
        } label: {
            Image(systemName: symbol)
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(binding.wrappedValue ? AkzioColor.primaryGold : AkzioColor.mutedText)
                .frame(width: 24, height: 22)
                .background {
                    if binding.wrappedValue {
                        RoundedRectangle(cornerRadius: 6, style: .continuous)
                            .fill(AkzioColor.gold(0.14))
                    }
                }
        }
        .buttonStyle(PressableButtonStyle())
        .help(L10n.text(title, language: language))
    }
}
