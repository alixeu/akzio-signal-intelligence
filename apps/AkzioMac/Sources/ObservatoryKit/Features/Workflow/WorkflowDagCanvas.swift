import SwiftUI

// MARK: - Workflow DAG
//
// Six edge kinds and nine node states in one Canvas. Zoom and pan only change the
// transform — the layout is never recomputed, so dragging stays free.
struct WorkflowDagCanvas: View {
    let workflow: WorkflowPresentation
    let selectedStageID: String?
    let namespace: Namespace.ID?
    let showsLabels: Bool
    let showsParticles: Bool
    let highlightsCriticalPath: Bool
    @Binding var scale: CGFloat
    @Binding var offset: CGSize
    let onSelect: (String) -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.akzioLabelDensity) private var labelDensity
    @Environment(\.appLanguage) private var language
    @State private var appearTime: Double?
    @GestureState private var dragTranslation: CGSize = .zero

    private var layout: DagLayout { DagLayout.layout(for: workflow.nodes) }

    var body: some View {
        GeometryReader { proxy in
            AmbientCanvas { time in
                let elapsed = growthElapsed(now: time)
                let fittedScale = layout.fitScale(in: proxy.size) * scale
                ZStack(alignment: .topLeading) {
                    Canvas(rendersAsynchronously: true) { context, _ in
                        drawEdges(&context, elapsed: elapsed, time: time)
                        drawNodes(&context, elapsed: elapsed, time: time)
                    }
                    .frame(width: layout.contentSize.width, height: layout.contentSize.height)
                    hitLayer
                }
                .frame(width: layout.contentSize.width, height: layout.contentSize.height)
                // Fit first, then apply the user's zoom on top. Without the fit the
                // 16-stage plan runs off the right edge of the card.
                // Anchor at the centre: `scaleEffect` does not change the layout size, so
                // scaling from a corner would leave the graph offset inside its frame.
                .scaleEffect(fittedScale, anchor: .center)
                // `position` gives the content one stable centre in the viewport.
                // The old frame-after-scale chain shifted the visual graph without
                // shifting its semantic layout, which made dense plans look misaligned.
                .position(
                    x: proxy.size.width / 2 + offset.width + dragTranslation.width,
                    y: proxy.size.height / 2 + offset.height + dragTranslation.height
                )
                .contentShape(Rectangle())
                .gesture(pan)
                .onAppear { if appearTime == nil { appearTime = time } }
            }
        }
        .clipped()
        .accessibilityElement(children: .contain)
        .accessibilityLabel("\(L10n.text("Workflow graph", language: language)), \(workflow.nodes.count) \(L10n.text("stages", language: language))")
    }

    private var pan: some Gesture {
        DragGesture()
            .updating($dragTranslation) { value, state, _ in state = value.translation }
            .onEnded { value in
                offset.width += value.translation.width
                offset.height += value.translation.height
            }
    }

    /// Path growth runs once on entry. With motion reduced the graph is already whole.
    private func growthElapsed(now: Double) -> Double {
        guard canvas.runsAmbient, let appearTime else { return 99 }
        return max(0, now - appearTime)
    }

    // MARK: Edges

    private func drawEdges(_ context: inout GraphicsContext, elapsed: Double, time: Double) {
        for (index, edge) in workflow.edges.enumerated() {
            guard let from = layout.point(edge.from), let to = layout.point(edge.to) else { continue }
            let growth = PathGrowth.staggered(index: index % 6, elapsed: elapsed, policy: policy)
            let target = CGPoint(
                x: from.x + (to.x - from.x) * growth.progress,
                y: from.y + (to.y - from.y) * growth.progress
            )
            var path = Path()
            path.move(to: from)
            path.addQuadCurve(
                to: target,
                control: CGPoint(x: (from.x + target.x) / 2, y: from.y + (target.y - from.y) * 0.15)
            )
            let emphasised = highlightsCriticalPath && edge.kind == .criticalPath
            context.stroke(
                path,
                with: .color(edge.kind.tone.color.opacity(emphasised ? 0.85 : 0.34)),
                style: StrokeStyle(
                    lineWidth: emphasised ? 1.8 : 1.1,
                    lineCap: .round,
                    dash: edge.kind.isDashed ? [4, 5] : []
                )
            )
            if edge.kind == .conflict {
                context.stroke(
                    path,
                    with: .color(AkzioColor.coral(0.55)),
                    style: StrokeStyle(lineWidth: 1.4, dash: [2, 3])
                )
            }
            if showsParticles, canvas.runsAmbient, converges(edge) {
                drawConvergence(&context, from: from, to: to, time: time)
            }
        }
    }

    /// Analyst → Synthesizer is the only convergence that carries particles.
    private func converges(_ edge: WorkflowEdgePresentation) -> Bool {
        edge.to == WorkflowStageKind.synthesizer.id && workflow.node(id: edge.to)?.isActive == true
    }

    private func drawConvergence(
        _ context: inout GraphicsContext,
        from: CGPoint,
        to: CGPoint,
        time: Double
    ) {
        let count = max(2, canvas.pathParticleBudget / 20)
        for index in 0..<count {
            let phase = (time / 0.9 + Double(index) / Double(count)).truncatingRemainder(dividingBy: 1)
            let eased = phase * phase
            let point = CGPoint(x: from.x + (to.x - from.x) * eased, y: from.y + (to.y - from.y) * eased)
            let size = 2.6 * (1 - phase) + 1
            context.fill(
                Path(ellipseIn: CGRect(x: point.x - size / 2, y: point.y - size / 2, width: size, height: size)),
                with: .color(AkzioColor.gold(0.7 * (1 - phase * 0.5)))
            )
        }
    }

    // MARK: Nodes

    private func drawNodes(_ context: inout GraphicsContext, elapsed: Double, time: Double) {
        let phase = (time.truncatingRemainder(dividingBy: Motion.pulsePeriod)) / Motion.pulsePeriod
        let breath = canvas.runsAmbient ? 0.5 - 0.5 * cos(phase * 2 * .pi) : 0.4
        // Conflict pulse is a decaying one-shot, not a loop.
        let conflictPulse = max(0, 1 - elapsed / 0.7)

        for node in workflow.nodes {
            guard let point = layout.point(node.id) else { continue }
            let radius = DagLayout.radius(for: node)
            let rect = CGRect(x: point.x - radius, y: point.y - radius, width: radius * 2, height: radius * 2)
            let tone = node.status.style.tone

            if node.isActive {
                let bloom = radius + 12 + 5 * breath
                context.fill(
                    Path(ellipseIn: CGRect(x: point.x - bloom, y: point.y - bloom, width: bloom * 2, height: bloom * 2)),
                    with: .radialGradient(
                        Gradient(colors: [AkzioColor.gold(0.30 - 0.14 * breath), .clear]),
                        center: point,
                        startRadius: radius * 0.8,
                        endRadius: bloom
                    )
                )
            }
            if node.isBlocked || node.stage == .critic && node.status.isLive {
                let ring = radius + 6 + 6 * conflictPulse
                context.stroke(
                    Path(ellipseIn: CGRect(x: point.x - ring, y: point.y - ring, width: ring * 2, height: ring * 2)),
                    with: .color(AkzioColor.coral(0.15 + 0.55 * conflictPulse)),
                    lineWidth: 1.4
                )
            }

            context.fill(Path(ellipseIn: rect), with: .color(fill(node)))
            context.stroke(
                Path(ellipseIn: rect),
                with: .color(tone.color.opacity(node.status.isLive ? 0.95 : 0.5)),
                style: StrokeStyle(
                    lineWidth: node.id == selectedStageID ? 2 : 1.2,
                    dash: node.status == .notTriggered || node.status == .notApplicable ? [3, 3] : []
                )
            )

            var symbol = context.resolve(
                Text(Image(systemName: node.stage.symbol))
                    .font(.system(size: 11, weight: .medium))
                    .foregroundColor(tone.color)
            )
            symbol.shading = .color(tone.color)
            context.draw(symbol, at: point)

            if showsLabels, labelDensity != .minimal || node.isActive {
                var label = context.resolve(
                    Text(L10n.text(node.stage.displayName, language: language))
                        .font(AkzioFont.caption)
                        .foregroundColor(node.isActive ? AkzioColor.primaryText : AkzioColor.secondaryText)
                )
                label.shading = .color(node.isActive ? AkzioColor.primaryText : AkzioColor.secondaryText)
                context.draw(label, at: CGPoint(x: point.x, y: point.y + radius + 11))

                if node.status == .notTriggered || node.status == .notApplicable {
                    var note = context.resolve(
                        Text(L10n.text(node.status.style.label, language: language))
                            .font(AkzioFont.caption)
                            .foregroundColor(AkzioColor.mutedText)
                    )
                    note.shading = .color(AkzioColor.mutedText)
                    context.draw(note, at: CGPoint(x: point.x, y: point.y + radius + 23))
                }
            }
            drawGateMark(&context, node: node, at: point, radius: radius)
        }
    }

    /// A passed gate gets a check; a rejected gate gets a coral bar. Never colour alone.
    private func drawGateMark(
        _ context: inout GraphicsContext,
        node: WorkflowNodePresentation,
        at point: CGPoint,
        radius: CGFloat
    ) {
        let isGate = node.stage == .evidenceGate || node.stage == .decisionGate || node.stage == .executionGate
        guard isGate else { return }
        let mark: String? = node.isBlocked ? "exclamationmark" : (node.taskStatus == .succeeded ? "checkmark" : nil)
        guard let mark else { return }
        let color = node.isBlocked ? AkzioColor.actionCoral : AkzioColor.primaryGold
        var badge = context.resolve(
            Text(Image(systemName: mark))
                .font(.system(size: 8, weight: .bold))
                .foregroundColor(color)
        )
        badge.shading = .color(color)
        context.draw(badge, at: CGPoint(x: point.x + radius - 2, y: point.y - radius + 2))
    }

    private func fill(_ node: WorkflowNodePresentation) -> Color {
        switch node.status {
        case .running, .observing: AkzioColor.gold(0.30)
        case .succeeded, .completed: AkzioColor.raisedSurface
        case .failed, .blocked, .rejected: AkzioColor.coral(0.18)
        case .stale: AkzioColor.coral(0.12)
        case .notTriggered, .notApplicable, .skipped, .cancelled: AkzioColor.deepBackground
        case .leased, .queued, .waiting: AkzioColor.deepBackground
        default: AkzioColor.raisedSurface
        }
    }

    // MARK: Hit layer

    private var hitLayer: some View {
        ZStack(alignment: .topLeading) {
            ForEach(workflow.nodes) { node in
                if let point = layout.point(node.id) {
                    let radius = DagLayout.radius(for: node)
                    Button { onSelect(node.id) } label: {
                        Circle()
                            .fill(Color.white.opacity(0.001))
                            .frame(width: radius * 2 + 8, height: radius * 2 + 8)
                    }
                    .buttonStyle(.plain)
                    .position(point)
                    .help("\(L10n.text(node.stage.displayName, language: language)) — \(L10n.text(node.status.style.label, language: language))")
                    .accessibilityLabel(L10n.text(node.stage.displayName, language: language))
                    .accessibilityValue(L10n.text(node.status.style.label, language: language))
                    .sharedElement(
                node.isActive ? .currentNode : .noSharedElement,
                        in: node.isActive ? namespace : nil
                    )
                }
            }
        }
    }
}
