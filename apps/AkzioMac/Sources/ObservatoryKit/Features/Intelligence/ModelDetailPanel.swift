import SwiftUI

// MARK: - Selected model detail
//
// Shows what the model *cited*, never how it thought: alternatives it weighed, named
// uncertainties, and the artifacts the conclusion rests on.
struct ModelDetailPanel: View {
    let council: CouncilPresentation
    let namespace: Namespace.ID?

    @Environment(\.motionPolicy) private var policy
    @State private var selectedArtifact: String?

    private var card: RoleCardPresentation? { council.selectedCard }

    var body: some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s3) {
            header
            HairlineDivider()
            Text(council.selectedModelSummary)
                .akzioText(.bodySmall)
                .staggeredReveal(index: 0)
            path.staggeredReveal(index: 1)
            alternatives
            uncertainties
            artifacts
            overall
            Spacer(minLength: 0)
        }
        .padding(AkzioLayout.s4)
        .frame(width: 344, alignment: .leading)
        .akzioGlass(.elevated)
    }

    private var header: some View {
        HStack(alignment: .top, spacing: AkzioLayout.s2) {
            Image(systemName: council.selectedRole.symbol)
                .font(.system(size: 15, weight: .medium))
                .foregroundStyle(AkzioColor.primaryGold)
                .frame(width: 22, height: 22)
            VStack(alignment: .leading, spacing: 2) {
                Text(council.selectedRole.displayName)
                    .akzioText(.title)
                Text(council.selectedModelName)
                    .akzioMono(11, color: AkzioColor.secondaryText)
                    .sharedElement(.modelName, in: namespace)
            }
            Spacer(minLength: 0)
            if let card {
                ProgressRing(
                    progress: PpmFormatter.fraction(ppm: card.confidencePpm),
                    tone: .gold,
                    lineWidth: 4,
                    diameter: 46
                ) {
                    Text(PpmFormatter.share(ppm: card.confidencePpm, fractionDigits: 0))
                        .akzioMono(9, color: AkzioColor.primaryText)
                }
                .frame(width: 46, height: 46)
                .sharedElement(.confidenceRing, in: namespace)
            }
        }
        .animation(policy.resolve(Motion.cardDetail), value: council.selectedRole)
    }

    private var path: some View {
        HStack(spacing: 4) {
            ForEach(Array(council.selectedPath.enumerated()), id: \.offset) { index, step in
                if index > 0 {
                    Image(systemName: "chevron.right")
                        .font(.system(size: 7, weight: .semibold))
                        .foregroundStyle(AkzioColor.mutedText)
                }
                Text(step).akzioText(.caption)
            }
        }
    }

    private var alternatives: some View {
        VStack(alignment: .leading, spacing: 5) {
            Text("Alternatives").akzioText(.caption)
            ForEach(Array(council.alternatives.enumerated()), id: \.element.id) { index, item in
                HStack(spacing: 6) {
                    Text(item.tag)
                        .akzioMono(10, color: AkzioColor.primaryGold)
                        .frame(width: 12)
                    Text(item.label).akzioText(.bodySmall)
                    Spacer(minLength: 4)
                    Text(PpmFormatter.share(ppm: item.matchPpm, fractionDigits: 0))
                        .akzioMono(10, color: AkzioColor.mutedText)
                }
                .staggeredReveal(index: index + 2)
            }
        }
    }

    private var uncertainties: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Uncertainties").akzioText(.caption)
            if council.uncertainties.isEmpty {
                UnavailableValue(.unavailable, size: 11)
            } else {
                ForEach(council.uncertainties) { item in
                    VStack(alignment: .leading, spacing: 3) {
                        HStack {
                            Text(item.label).akzioText(.bodySmall)
                            Spacer(minLength: 4)
                            Text(PpmFormatter.share(ppm: item.weightPpm, fractionDigits: 0))
                                .akzioMono(10, color: AkzioColor.mutedText)
                        }
                        RatioBar(fraction: PpmFormatter.fraction(ppm: item.weightPpm), height: 4)
                    }
                }
            }
        }
    }

    private var artifacts: some View {
        VStack(alignment: .leading, spacing: 5) {
            Text("Basis Artifacts").akzioText(.caption)
            LazyVGrid(
                columns: [GridItem(.adaptive(minimum: 132), alignment: .leading)],
                alignment: .leading,
                spacing: 5
            ) {
                ForEach(council.basisArtifacts) { artifact in
                    Chip(
                        artifact.label,
                        symbol: artifact.symbol,
                        kind: .artifact,
                        isSelected: selectedArtifact == artifact.id
                    ) {
                        withAnimation(policy.resolve(Motion.control)) {
                            selectedArtifact = selectedArtifact == artifact.id ? nil : artifact.id
                        }
                    }
                }
            }
        }
    }

    private var overall: some View {
        HStack(spacing: AkzioLayout.s2) {
            Text("Overall Uncertainty").akzioText(.caption)
            Spacer(minLength: 4)
            if let value = council.overallUncertaintyPpm {
                Text(PpmFormatter.share(ppm: value))
                    .akzioMono(12, color: AkzioColor.primaryText)
                    .akzioNumeric(value, policy: policy)
            } else {
                UnavailableValue(.unavailable, size: 11)
            }
        }
    }
}
