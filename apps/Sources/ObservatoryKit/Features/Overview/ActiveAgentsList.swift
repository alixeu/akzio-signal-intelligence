import SwiftUI

// MARK: - Active agents
//
// Running agents float to the top. Reordering is a spring on the row's geometry, so
// rows glide past each other instead of blinking into new slots.
struct ActiveAgentsList: View {
    let agents: [AgentRailItem]
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Namespace private var rows

    private var sorted: [AgentRailItem] {
        agents.sorted { lhs, rhs in
            if lhs.status.isLive != rhs.status.isLive { return lhs.status.isLive }
            if lhs.progressPpm != rhs.progressPpm { return lhs.progressPpm > rhs.progressPpm }
            return lhs.id < rhs.id
        }
    }

    var body: some View {
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
        sorted.first { $0.role == agent.role }?.id == agent.id
    }

    private func row(_ agent: AgentRailItem) -> some View {
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
