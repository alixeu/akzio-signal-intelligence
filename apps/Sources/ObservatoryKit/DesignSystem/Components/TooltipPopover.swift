import SwiftUI

// MARK: - Tooltip
//
// Glass surface anchored to its trigger: it scales out of the trigger edge, not
// out of its own centre. First tooltip waits ~400ms; while one is already open,
// neighbours appear instantly.
public struct TooltipPopover<Label: View, Content: View>: View {
    private let label: Label
    private let content: Content
    private let edge: Edge
    private let instant: Bool

    @Environment(\.motionPolicy) private var policy
    @State private var isVisible = false
    @State private var hoverTask: Task<Void, Never>?

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
        switch edge {
        case .top: .bottom
        case .bottom: .top
        case .leading: .trailing
        case .trailing: .leading
        }
    }

    public var body: some View {
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
        switch edge {
        case .top: .top
        case .bottom: .bottom
        case .leading: .leading
        case .trailing: .trailing
        }
    }

    private var offset: CGSize {
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
    private let status: AkzioStatus
    private let overrideDetail: String?
    @Environment(\.appLanguage) private var language

    public init(_ status: AkzioStatus, detail: String? = nil) {
        self.status = status
        self.overrideDetail = detail
    }

    public var body: some View {
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
    private let kind: MissingValue
    private let size: CGFloat
    @Environment(\.appLanguage) private var language

    public init(_ kind: MissingValue = .unavailable, size: CGFloat = 13) {
        self.kind = kind
        self.size = size
    }

    public var body: some View {
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
