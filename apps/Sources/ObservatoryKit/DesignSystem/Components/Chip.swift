import SwiftUI

// 文件职责：提供可选交互的筛选/标签 Chip，以及由 Binding 持有选择状态的分段控件。
// 组件只保存不可变输入和短暂 hover 状态；语言、动效和父级选择通过 Environment/Binding 流入。
// MARK: - Chip
//
// Filter and tag chips. Selection turns the border gold and expands the chip
// slightly (180–240ms) — no colour-only signalling.
public struct Chip: View {
    // Kind 是值语义的布局选择；action 为 nil 时组件保持纯展示，不创建按钮语义。
    public enum Kind { case filter, tag, artifact }

    private let title: String
    private let symbol: String?
    private let kind: Kind
    private let isSelected: Bool
    private let action: (() -> Void)?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @State private var isHovering = false

    // 初始化复制标题、图标、样式和可选闭包；action 的所有权由该 View 值保存，执行仍由 SwiftUI Button 触发。
    public init(
        _ title: String,
        symbol: String? = nil,
        kind: Kind = .filter,
        isSelected: Bool = false,
        action: (() -> Void)? = nil
    ) {
        self.title = title
        self.symbol = symbol
        self.kind = kind
        self.isSelected = isSelected
        self.action = action
    }

    public var body: some View {
        // body 从 Environment 读取语言/动效，并用 State 只记录 hover；父级传入的 isSelected 不在本地改写。
        // 先构造不带交互的内容，再按 action 是否存在包装 Button；无 action 的 tag 不会伪造点击语义。
        let content = HStack(spacing: 5) {
            if let symbol {
                Image(systemName: symbol)
                    .font(.system(size: 9, weight: .medium))
            }
            Text(L10n.text(title, language: language))
                .font(AkzioFont.label)
                .tracking(AkzioFont.labelTracking)
                .lineLimit(1)
        }
        .foregroundStyle(isSelected ? AkzioColor.primaryGold : AkzioColor.secondaryText)
        .padding(.horizontal, kind == .tag ? 7 : 9)
        .padding(.vertical, kind == .tag ? 3 : 5)
        .background(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .fill(isSelected ? AkzioColor.primaryGold.opacity(0.12) : AkzioColor.elevatedSurface.opacity(isHovering ? 0.9 : 0.55))
        )
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.chipRadius, style: .continuous)
                .strokeBorder(
                    isSelected ? AkzioColor.goldHairline.opacity(2.2) : AkzioColor.hairline,
                    lineWidth: AkzioLayout.hairlineWidth
                )
        )
        .scaleEffect(isSelected ? 1.03 : 1.0)
        .animation(policy.resolve(Motion.hover), value: isSelected)
        .animation(policy.resolve(Motion.hover), value: isHovering)
        .onHover { hovering in
            // onHover 闭包接收指针状态，写回局部 State；它不把 hover 传给业务设置。
            // Hover only exists for precise pointers; trackpad taps must not latch it.
            isHovering = hovering
        }

        if let action {
            // Optional closure 为 Some 时才建立 Button；Button 负责调用 action，View 不猜测业务副作用。
            Button(action: action) { content }
                .buttonStyle(PressableButtonStyle())
                .accessibilityAddTraits(isSelected ? [.isSelected] : [])
        } else {
            content
        }
    }
}

// MARK: - Segmented control

/// Selection background slides between options instead of flashing.
public struct AkzioSegmentedControl<Value: Hashable>: View {
    // 泛型 Value 只需 Hashable 便可作为 ForEach identity；实际选择由 Binding 的拥有者维护。
    private let options: [(value: Value, label: String)]
    @Binding private var selection: Value

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Namespace private var indicator

    // 输入外部 Binding 和 option 值数组；初始化只复制 options，selection 仍通过 property wrapper 连接父 View。
    public init(selection: Binding<Value>, options: [(value: Value, label: String)]) {
        self._selection = selection
        self.options = options
    }

    public var body: some View {
        // body 读取当前 selection；点击闭包在同一动画事务中写回 Binding，让父级成为状态唯一来源。
        // 选项集合只负责呈现；点击在同一个 withAnimation 中写入 Binding，父 View 仍是选择状态的所有者。
        HStack(spacing: 2) {
            ForEach(options, id: \.value) { option in
                // ForEach 闭包借用单个 option，局部 isSelected 只用于渲染，不产生第二份选择状态。
                let isSelected = option.value == selection
                Button {
                    // withAnimation 闭包只写入 Binding；matched geometry 观察这个值变化完成指示器移动。
                    withAnimation(policy.resolve(Motion.selection)) { selection = option.value }
                } label: {
                    Text(L10n.text(option.label, language: language))
                        .font(AkzioFont.label)
                        .tracking(AkzioFont.labelTracking)
                        .foregroundStyle(isSelected ? AkzioColor.primaryGold : AkzioColor.secondaryText)
                        .padding(.horizontal, 10)
                        .padding(.vertical, 5)
                        .background {
                            if isSelected {
                                RoundedRectangle(cornerRadius: 6, style: .continuous)
                                    .fill(AkzioColor.primaryGold.opacity(0.14))
                                    .matchedGeometryEffect(id: "segment", in: indicator)
                            }
                        }
                }
                .buttonStyle(.plain)
                .accessibilityAddTraits(isSelected ? [.isSelected] : [])
            }
        }
        .padding(2)
    .akzioGlassBackdrop(AkzioColor.deepBackground, radius: 8)
        .overlay(
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .strokeBorder(AkzioColor.hairline, lineWidth: AkzioLayout.hairlineWidth)
        )
    }
}
