import SwiftUI

// MARK: - Role card
//
// One card per council role. Hover lifts 4pt with a ≤2° tilt and a warm edge light;
// selecting a card hands its icon, role and model over to the detail panel.
struct RoleCardView: View {
    let card: RoleCardPresentation
    let isSelected: Bool
    /// Shared namespace for the Overview/Workflow → Intelligence handoff. The
    /// per-role ID is what lets the active agent land on its own card.
    let routeNamespace: Namespace.ID?
    let onSelect: () -> Void

    @Environment(\.motionPolicy) private var policy

    var body: some View {
        Button(action: onSelect) {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                header
                HairlineDivider()
                metrics
                confidence
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .akzioCard(
            border: isSelected ? AkzioColor.goldHairline : AkzioColor.hairline
            )
            .contentShape(Rectangle())
        }
        .buttonStyle(PressableButtonStyle(scale: 0.985))
        .hoverLift(lift: 5, tilt: 1.8)
        .sharedElement(.roleCard(card.role), in: routeNamespace)
        .accessibilityLabel("\(card.role.displayName), \(card.status.style.label)")
        .accessibilityAddTraits(isSelected ? [.isSelected] : [])
    }

    private var header: some View {
        HStack(alignment: .top, spacing: AkzioLayout.s2) {
            Image(systemName: card.role.symbol)
                .font(.system(size: 13, weight: .medium))
                .foregroundStyle(card.isNotTriggered ? AkzioColor.mutedText : AkzioColor.primaryGold)
                .frame(width: 20, height: 20)
            VStack(alignment: .leading, spacing: 2) {
                Text(card.role.displayName)
                    .akzioText(.sectionTitle)
                Text(card.role.responsibility).akzioText(.caption)
                Text(card.model)
                    .akzioMono(10, color: AkzioColor.secondaryText)
            }
            Spacer(minLength: 0)
        }
    }

    private var metrics: some View {
        VStack(alignment: .leading, spacing: 4) {
            StatusBadge(card.status, size: .compact)
            if card.isNotTriggered {
                Text(card.status.detail ?? "").akzioText(.caption)
            } else {
                metricRow("Tokens In", PpmFormatter.count(card.tokensIn))
                metricRow("Tokens Out", PpmFormatter.count(card.tokensOut))
                metricRow("Tool Calls", PpmFormatter.count(card.toolCalls))
                metricRow("Latency", PpmFormatter.latency(millis: card.latencyMillis))
            }
        }
    }

    private func metricRow(_ label: String, _ value: String) -> some View {
        HStack(spacing: 4) {
            Text(label).akzioText(.caption)
            Spacer(minLength: 4)
            Text(value)
                .akzioMono(10, color: AkzioColor.primaryText)
                .akzioNumeric(value, policy: policy)
        }
    }

    @ViewBuilder
    private var confidence: some View {
        if card.isNotTriggered {
            EmptyView()
        } else {
            HStack(spacing: AkzioLayout.s2) {
                RatioBar(
                    fraction: PpmFormatter.fraction(ppm: card.confidencePpm),
                    tone: .gold,
                    height: 4
                )
                Text(PpmFormatter.share(ppm: card.confidencePpm, fractionDigits: 0))
                    .akzioMono(10, color: AkzioColor.primaryText)
                    .akzioNumeric(card.confidencePpm ?? -1, policy: policy)
            }
        }
    }
}
