import SwiftUI

// 文件职责：封装按压、hover 抬升和行高亮三类 ViewModifier，统一从 MotionPolicy 解析动效。
// Modifier 输入是值配置和 Content，输出新的 some View；State 只保存瞬时 hover，不承担业务选择。
// MARK: - Press feedback
//
// Feedback lands on press-down, not on release: 0.97 scale, ~160ms.
public struct PressableButtonStyle: ButtonStyle {
    @Environment(\.motionPolicy) private var policy
    private let scale: CGFloat

    // 初始化保存按压缩放值；ButtonStyle 的 configuration 在 makeBody 时由 SwiftUI 提供。
    public init(scale: CGFloat = 0.97) {
        self.scale = scale
    }

    public func makeBody(configuration: Configuration) -> some View {
        // 输入 Button configuration 和 Environment policy，输出带按压 scale/animation/contentShape 的 label。
        // ButtonStyle 只读取 SwiftUI 提供的 isPressed；释放时状态自动恢复，调用方不需要清理。
        configuration.label
            .scaleEffect(configuration.isPressed ? scale : 1)
            .animation(policy.resolve(Motion.control), value: configuration.isPressed)
            .contentShape(Rectangle())
    }
}

// MARK: - Hover lift

/// Card hover: 3–6pt lift, ≤2° tilt, warm edge light sweeping top-left → bottom-right.
public struct HoverLift: ViewModifier {
    // lift/tilt/radius 是不可变值配置；isHovering 是 Modifier 自己的短暂交互状态。
    let lift: CGFloat
    let tilt: Double
    let radius: CGFloat

    @Environment(\.motionPolicy) private var policy
    @State private var isHovering = false

    // body 用 policy.travel 调整位移，用 policy.isReduced 决定是否绘制倾斜和边缘光；content 始终原样保留。
    public func body(content: Content) -> some View {
        // hover 只影响装饰层、位移和阴影；减少动效时保留 hover 状态但不应用 3D 倾斜。
        content
            .overlay {
                if isHovering && !policy.isReduced {
                    RoundedRectangle(cornerRadius: radius, style: .continuous)
                        .strokeBorder(
                            LinearGradient(
                                colors: [
                                    AkzioColor.primaryGold.opacity(0.55),
                                    AkzioColor.primaryGold.opacity(0.10),
                                    .clear,
                                ],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            ),
                            lineWidth: 1
                        )
                        .allowsHitTesting(false)
                }
            }
            .offset(y: isHovering ? -policy.travel(lift) : 0)
            .rotation3DEffect(
                .degrees(isHovering && !policy.isReduced ? tilt : 0),
                axis: (x: 1, y: -0.35, z: 0),
                perspective: 0.6
            )
            .shadow(
                color: .black.opacity(isHovering ? 0.34 : 0),
                radius: isHovering ? 14 : 0,
                y: isHovering ? 6 : 0
            )
            .animation(policy.resolve(Motion.hover), value: isHovering)
            .onHover { isHovering = $0 }
    }
}

extension View {
    // 这些入口只构造 Modifier；调用方通过参数选择 lift/tilt/选中态，不直接接触内部 State。
    public func hoverLift(
        lift: CGFloat = 4,
        tilt: Double = 1.6,
        radius: CGFloat = AkzioLayout.cardRadius
    ) -> some View {
        modifier(HoverLift(lift: lift, tilt: tilt, radius: radius))
    }

    /// Row hover: background warms, row height never changes.
    public func rowHoverHighlight(isSelected: Bool = false) -> some View {
        modifier(RowHoverHighlight(isSelected: isSelected))
    }
}

public struct RowHoverHighlight: ViewModifier {
    let isSelected: Bool

    @Environment(\.motionPolicy) private var policy
    @State private var isHovering = false

    // body 让 selected 优先于 hover，保持行高不变；Environment policy 只改变过渡，不改变选中数据。
    public func body(content: Content) -> some View {
        // 行高不随 hover 改变；选中态优先于 hover，并用左侧色条提供非颜色之外的定位。
        content
            .background {
                if isSelected {
                    LinearGradient(
                        colors: [AkzioColor.primaryGold.opacity(0.14), AkzioColor.primaryGold.opacity(0.05)],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                } else if isHovering {
                    LinearGradient(
                        colors: [AkzioColor.primaryGold.opacity(0.07), .clear],
                        startPoint: .leading,
                        endPoint: .trailing
                    )
                }
            }
            .overlay(alignment: .leading) {
                if isSelected {
                    Rectangle()
                        .fill(AkzioColor.primaryGold)
                        .frame(width: 2)
                }
            }
            .offset(y: isSelected ? -1 : 0)
            .animation(policy.resolve(Motion.hover), value: isHovering)
            .animation(policy.resolve(Motion.selection), value: isSelected)
            .onHover { isHovering = $0 }
    }
}
