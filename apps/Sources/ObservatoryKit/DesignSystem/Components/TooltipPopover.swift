import SwiftUI

// 文件职责：提供与触发器锚定的延迟 tooltip，以及条件状态说明和缺失值的统一展示。
// Label/Content 是 ViewBuilder 物化的值；hover Task 只管理延迟生命周期，不承担业务数据或持久化。
// MARK: - Tooltip
//
// Glass surface anchored to its trigger: it scales out of the trigger edge, not
// out of its own centre. First tooltip waits ~400ms; while one is already open,
// neighbours appear instantly.
public struct TooltipPopover<Label: View, Content: View>: View {
    // generic Label/Content 保持调用方的静态 View 类型；edge/instant 是不可变展示配置。
    private let label: Label
    private let content: Content
    private let edge: Edge
    private let instant: Bool

    @Environment(\.motionPolicy) private var policy
    @State private var isVisible = false
    @State private var hoverTask: Task<Void, Never>?

    // 两个 builder 闭包在 init 时生成 label/content；Task 只在 hover 时创建，离开时通过 cancel 收回延迟工作。
    public init(
        edge: Edge = .top,
        instant: Bool = false,
        @ViewBuilder label: () -> Label,
        @ViewBuilder content: () -> Content
    ) {
        self.edge = edge
        self.instant = instant
        self.label = label()
        self.content = content()
    }

    private var anchor: UnitPoint {
        // computed property 将展示方向映射为 scale anchor，使 tooltip 从触发器边缘而非自身中心出现。
        switch edge {
        case .top: .bottom
        case .bottom: .top
        case .leading: .trailing
        case .trailing: .leading
        }
    }

    public var body: some View {
        // body 以 isVisible 控制 overlay；hover 闭包取消旧 Task、按 instant 决定延迟，并只写回本地 State。
        // hover 时取消旧 Task 再启动新 Task；离开立即隐藏，延迟任务即使稍后醒来也会因取消而不显示。
        label
            .overlay(alignment: alignment) {
                if isVisible {
                    content
                        .padding(.horizontal, 9)
                        .padding(.vertical, 7)
                        .akzioGlass(.elevated, radius: 9)
                        .fixedSize()
                        .scaleEffect(isVisible ? 1 : 0.96, anchor: anchor)
                        .opacity(isVisible ? 1 : 0)
                        .offset(offset)
                        .transition(.opacity)
                        .allowsHitTesting(false)
                }
            }
            .onHover { hovering in
                hoverTask?.cancel()
                if hovering {
                    let delay: Duration = instant ? .milliseconds(0) : .milliseconds(380)
                    hoverTask = Task {
                        // Task 闭包只等待 Duration；取消或醒来后均先检查 isCancelled，避免过期 hover 打开 tooltip。
                        // Task 的 sleep 只负责延迟，不持有业务资源；SwiftUI 状态写回仍发生在当前 actor。
                        try? await Task.sleep(for: delay)
                        guard !Task.isCancelled else { return }
                        withAnimation(policy.resolve(.easeOut(duration: 0.14))) { isVisible = true }
                    }
                } else {
                    withAnimation(policy.resolve(.easeOut(duration: 0.12))) { isVisible = false }
                }
            }
    }

    private var alignment: Alignment {
        // computed property 将 Edge 转成 overlay 对齐点，与 anchor/offset 保持同一方向语义。
        switch edge {
        case .top: .top
        case .bottom: .bottom
        case .leading: .leading
        case .trailing: .trailing
        }
    }

    private var offset: CGSize {
        // computed property 返回固定方向偏移；它只影响布局，不改变 tooltip 内容或可访问文案。
        switch edge {
        case .top: CGSize(width: 0, height: -34)
        case .bottom: CGSize(width: 0, height: 34)
        case .leading: CGSize(width: -12, height: 0)
        case .trailing: CGSize(width: 12, height: 0)
        }
    }
}

// MARK: - Explanatory footnote

/// Renders the mandated wording for conditional / missing states, so no page
/// invents its own phrasing.
public struct StatusExplanation: View {
    // status 提供默认 detail，overrideDetail 可替换说明；nil 时整个 View 不占位。
    private let status: AkzioStatus
    private let overrideDetail: String?
    @Environment(\.appLanguage) private var language

    // 初始化保存状态与 Optional detail；body 通过 ?? 选择覆盖文案或状态默认文案。
    public init(_ status: AkzioStatus, detail: String? = nil) {
        self.status = status
        self.overrideDetail = detail
    }

    public var body: some View {
        // Optional binding 只在有说明时构造 HStack；缺失 detail 不被转换为空字符串或数值。
        // 只在有 detail 时渲染说明；nil 状态不会占位，也不会把 unavailable 误写成数值。
        if let detail = overrideDetail ?? status.detail {
            HStack(spacing: 5) {
                Image(systemName: status.style.symbol)
                    .font(.system(size: 9, weight: .medium))
                    .foregroundStyle(status.style.color)
                Text(L10n.text(detail, language: language))
                    .akzioText(.bodySmall, color: AkzioColor.mutedText)
            }
        }
    }
}

// MARK: - Empty / unavailable value

/// Single place that renders an absent value, so `0` can never leak in.
public struct UnavailableValue: View {
    // MissingValue 是 Sendable 值语义词汇；kind.status 提供对应图标和无障碍状态，而不是默认 0。
    private let kind: MissingValue
    private let size: CGFloat
    @Environment(\.appLanguage) private var language

    // 初始化复制缺失类别和字号；展示逻辑只读取这些值，不向数据源回写。
    public init(_ kind: MissingValue = .unavailable, size: CGFloat = 13) {
        self.kind = kind
        self.size = size
    }

    public var body: some View {
        // body 同时渲染语义图标、翻译文案和 accessibility label，保持缺失状态可见且可读。
        // 缺失值始终显示语义图标和文案；该 View 不把缺失转成 0 或空字符串。
        HStack(spacing: 4) {
            Image(systemName: kind.status.style.symbol)
                .font(.system(size: size * 0.72, weight: .medium))
            Text(L10n.text(kind.rawValue, language: language))
                .font(.system(size: size, weight: .medium))
        }
        .foregroundStyle(AkzioColor.mutedText)
        .accessibilityLabel(L10n.text(kind.rawValue, language: language))
    }
}
