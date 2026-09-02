import SwiftUI

// MARK: - Policy transitions
//
// Memory runs the five-state lifecycle; contracts and topologies climb the canary
// ladder. Contested is coral, Retired is grey, and the current step is the only one
// that lights up.
struct PolicyTransitionTrack: View {
    let tracks: [PolicyTrackPresentation]

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language

    var body: some View {
        SectionCard(title: "Policy Transitions", subtitle: "Memory lifecycle and canary ladder") {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                ForEach(Array(tracks.enumerated()), id: \.element.id) { index, track in
                    row(track).staggeredReveal(index: index)
                }
            }
        }
    }

    private func row(_ track: PolicyTrackPresentation) -> some View {
        VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Image(systemName: track.subject.symbol)
                    .font(.system(size: 11, weight: .medium))
                    .foregroundStyle(AkzioColor.secondaryText)
                VStack(alignment: .leading, spacing: 1) {
                    Text(track.name).akzioText(.body, color: AkzioColor.primaryText)
                    Text("\(track.subject.displayName) · N=\(track.sampleCount) · \(track.activeSinceLabel)")
                        .akzioText(.caption)
                }
                Spacer(minLength: AkzioLayout.s2)
                PillTag(track.stateLabel, tone: track.tone)
            }
            if let memory = track.memoryState {
                ladder(MemoryLifecycle.allCases.map(\.displayName), current: memory.displayName, tone: track.tone)
            } else if let candidate = track.candidateState {
                ladder(CandidatePolicyState.allCases.map(\.displayName), current: candidate.displayName, tone: track.tone)
            }
            metrics(track)
        }
    }

    private func ladder(_ steps: [String], current: String, tone: AkzioTone) -> some View {
        HStack(spacing: 0) {
            ForEach(Array(steps.enumerated()), id: \.offset) { index, step in
                let reachedIndex = steps.firstIndex(of: current) ?? 0
                let isCurrent = step == current
                let reached = index <= reachedIndex
                HStack(spacing: 0) {
                    if index > 0 {
                        Rectangle()
                            .fill(reached ? tone.color.opacity(0.5) : Color.white.opacity(0.06))
                            .frame(height: 1.2)
                    }
                    VStack(spacing: 3) {
                        Circle()
                            .fill(isCurrent ? tone.color : (reached ? tone.color.opacity(0.4) : Color.white.opacity(0.10)))
                            .frame(width: isCurrent ? 9 : 6, height: isCurrent ? 9 : 6)
                            .overlay {
                                if isCurrent {
                                    Circle()
                                        .strokeBorder(tone.color.opacity(0.55), lineWidth: 1)
                                        .frame(width: 15, height: 15)
                                }
                            }
                        Text(L10n.text(step, language: language))
                            .akzioText(.caption, color: isCurrent ? AkzioColor.secondaryText : AkzioColor.mutedText)
                            .lineLimit(1)
                            .fixedSize()
                    }
                    .frame(minWidth: 58)
                }
                .frame(maxWidth: .infinity)
            }
        }
        .animation(policy.resolve(.spring(response: 0.5, dampingFraction: 1.0)), value: current)
    }

    private func metrics(_ track: PolicyTrackPresentation) -> some View {
        HStack(spacing: AkzioLayout.s4) {
            metric("Win Rate", PpmFormatter.share(ppm: track.winRatePpm))
            metric("Net Impact", PpmFormatter.percent(ppm: track.netImpactPpm))
            metric("Stability", PpmFormatter.share(ppm: track.stabilityPpm))
            metric("Exposure", PpmFormatter.share(ppm: track.exposurePpm, fractionDigits: 0))
        }
    }

    private func metric(_ label: String, _ value: String) -> some View {
        HStack(spacing: 4) {
            Text(L10n.text(label, language: language)).akzioText(.caption)
            Text(value)
                .akzioMono(10, color: AkzioColor.primaryText)
                .akzioNumeric(value, policy: policy)
        }
    }
}
