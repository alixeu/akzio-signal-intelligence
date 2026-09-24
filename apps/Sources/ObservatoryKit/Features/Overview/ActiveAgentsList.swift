import SwiftUI

// MARK: - Active agents
//
// Running agents float to the top. Reordering is a spring on the row's geometry, so
// rows glide past each other instead of blinking into new slots.
struct ActiveAgentsList: View {
    // agents 是 Overview 的只读展示投影；namespace 可选，用于跨页面的角色卡共享元素。
    let agents: [AgentRailItem]
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    // rows 只负责列表内部的几何匹配，不承载业务数据或导航状态。
    @Namespace private var rows

    private var sorted: [AgentRailItem] {
        // 排序闭包先把运行中的 Agent 提到前面，再按进度和 ID 稳定排序。
        agents.sorted { lhs, rhs in
            if lhs.status.isLive != rhs.status.isLive { return lhs.status.isLive }
            if lhs.progressPpm != rhs.progressPpm { return lhs.progressPpm > rhs.progressPpm }
            return lhs.id < rhs.id
        }
    }

    var body: some View {
        // 列表读取 sorted 派生结果；每一行的共享元素和选择动画都不修改 agents。
        SectionCard(
            title: "Active Agents",
            subtitle: "\(agents.count) \(L10n.text("roles", language: language))",
            padding: AkzioLayout.s3
        ) {
            VStack(alignment: .leading, spacing: 7) {
                ForEach(sorted) { agent in
                    row(agent)
                        .matchedGeometryEffect(id: agent.id, in: rows)
                        // Per-role anchor: this row becomes that role's card on the
                        // Intelligence page. Only the first row of a role may be the
                        // source — three Analysts sharing one ID would collapse onto
                        // each other.
                        .sharedElement(
                            .roleCard(agent.role),
                            in: isRoleAnchor(agent) ? namespace : nil
                        )
                }
            }
            .animation(policy.resolve(Motion.selection), value: sorted.map(\.id))
        }
    }

    /// One anchor per role: the first row of that role in display order.
    private func isRoleAnchor(_ agent: AgentRailItem) -> Bool {
        // 闭包选择当前显示顺序中每个 role 的第一行，避免同角色锚点互相覆盖。
        sorted.first { $0.role == agent.role }?.id == agent.id
    }

    private func row(_ agent: AgentRailItem) -> some View {
        // 单行根据 status 决定显示标签或进度条，所有文本由环境语言本地化。
        HStack(spacing: AkzioLayout.s2) {
            StatusDot(agent.status)
            VStack(alignment: .leading, spacing: 1) {
                HStack(spacing: 5) {
                    Text(L10n.text(agent.name, language: language))
                        .akzioText(.body, color: AkzioColor.primaryText)
                        .lineLimit(1)
                        .layoutPriority(1)
                    Text(agent.model)
                        .akzioMono(10, color: AkzioColor.mutedText)
                        .lineLimit(1)
                        .truncationMode(.tail)
                }
                Text(L10n.text(agent.activityLabel, language: language)).akzioText(.caption)
                    .lineLimit(2).help(agent.activityLabel)
            }
            Spacer(minLength: AkzioLayout.s2)
            if agent.status == .notTriggered {
                PillTag(agent.status.style.label, tone: .muted)
            } else {
                HStack(spacing: AkzioLayout.s2) {
                    RatioBar(
                        fraction: PpmFormatter.fraction(ppm: agent.progressPpm),
                        tone: agent.status.style.tone,
                        height: 4
                    )
                    .frame(width: 62)
                    Text(PpmFormatter.share(ppm: agent.progressPpm, fractionDigits: 0))
                        .akzioMono(11, color: AkzioColor.primaryText)
                        .akzioNumeric(agent.progressPpm, policy: policy)
                        .frame(width: 40, alignment: .trailing)
                }
            }
        }
        .accessibilityElement(children: .combine)
            .accessibilityLabel(
                "\(L10n.text(agent.name, language: language)), \(L10n.text(agent.status.style.label, language: language))"
            )
    }
}
