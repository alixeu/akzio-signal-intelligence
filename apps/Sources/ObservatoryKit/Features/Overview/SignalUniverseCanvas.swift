import SwiftUI

// MARK: - Signal Universe
//
// One Canvas draws the orbits, edges, nodes and particles; a thin overlay of real
// Views sits on top purely for hit testing, tooltips, VoiceOver and shared-element
// anchors. That split keeps the field cheap while staying accessible.
struct SignalUniverseCanvas: View {
    @Environment(\.appLanguage) private var language
    let workflow: WorkflowPresentation
    let namespace: Namespace.ID?
    let selectedStageID: String?
    let onSelect: (String) -> Void

    @Environment(\.motionPolicy) private var policy
    @Environment(\.canvasRenderPolicy) private var canvas
    @Environment(\.akzioLabelDensity) private var labelDensity
    @State private var presentedStageID: String?

    private var nodes: [UniverseNode] { SignalUniverseLayout.nodes(from: workflow) }

    var body: some View {
        GeometryReader { proxy in
            ZStack(alignment: .topTrailing) {
                AmbientCanvas { time in
                    let rotation = SignalUniverseLayout.rotation(time: time, policy: canvas)
                    let placed = positions(in: proxy.size, rotation: rotation)
                    ZStack {
                        Canvas(rendersAsynchronously: true) { context, size in
                            drawOrbits(&context, size: size)
                            drawEdges(&context, placed: placed, time: time)
                            drawNodes(&context, placed: placed, time: time)
                        }
                        hitLayer(placed: placed)
                    }
                }
                stageOverlay(in: proxy.size)
            }
            .animation(policy.resolve(Motion.panel), value: presentedStageID)
        }
        .sharedElement(.signalUniverse, in: namespace)
        .accessibilityElement(children: .contain)
        .accessibilityLabel("\(L10n.text("Signal Universe", language: language)): \(nodes.count)")
    }

    private func positions(in size: CGSize, rotation: Double) -> [(node: UniverseNode, point: CGPoint)] {
        nodes.map { ($0, SignalUniverseLayout.position($0, in: size, rotation: rotation)) }
    }

    // MARK: Canvas passes

    private func drawOrbits(_ context: inout GraphicsContext, size: CGSize) {
        let center = CGPoint(x: size.width / 2, y: size.height / 2)
        for orbit in SignalUniverseLayout.orbits {
            let rect = CGRect(
                x: center.x - SignalUniverseLayout.extent(orbit, in: size).width,
                y: center.y - SignalUniverseLayout.extent(orbit, in: size).height,
                width: SignalUniverseLayout.extent(orbit, in: size).width * 2,
                height: SignalUniverseLayout.extent(orbit, in: size).height * 2
            )
            context.stroke(
                Path(ellipseIn: rect),
                with: .color(.white.opacity(0.05)),
                lineWidth: 1
            )
        }
        // Warm core: the run itself.
        let coreRect = CGRect(x: center.x - 26, y: center.y - 26, width: 52, height: 52)
        context.fill(
            Path(ellipseIn: coreRect),
            with: .radialGradient(
                Gradient(colors: [AkzioColor.gold(0.28), AkzioColor.gold(0.04), .clear]),
                center: center,
                startRadius: 0,
                endRadius: 26
            )
        )
        context.fill(
            Path(ellipseIn: CGRect(x: center.x - 3, y: center.y - 3, width: 6, height: 6)),
            with: .color(AkzioColor.primaryGold)
        )
    }

    private func drawEdges(
        _ context: inout GraphicsContext,
        placed: [(node: UniverseNode, point: CGPoint)],
        time: Double
    ) {
        let lookup = Dictionary(uniqueKeysWithValues: placed.map { ($0.node.id, $0) })
        for edge in workflow.edges {
            guard let from = lookup[edge.from], let to = lookup[edge.to] else { continue }
            var path = Path()
            path.move(to: from.point)
            path.addLine(to: to.point)
            let carriesSignal = from.node.status == .succeeded || from.node.isCurrent
            context.stroke(
                path,
                with: .color(edge.kind.tone.color.opacity(carriesSignal ? 0.30 : 0.12)),
                style: StrokeStyle(
                    lineWidth: edge.kind == .criticalPath ? 1.4 : 1,
                    dash: edge.kind.isDashed ? [3, 4] : []
                )
            )
            // Particles only travel the live edge, so the eye has one thing to follow.
            if to.node.isCurrent, canvas.runsAmbient {
                drawParticles(&context, from: from.point, to: to.point, time: time)
            }
        }
    }

    private func drawParticles(
        _ context: inout GraphicsContext,
        from: CGPoint,
        to: CGPoint,
        time: Double
    ) {
        let count = max(1, canvas.pathParticleBudget / 12)
        for index in 0..<count {
            let offset = Double(index) / Double(count)
            let progress = (time / 2.2 + offset).truncatingRemainder(dividingBy: 1)
            let point = CGPoint(
                x: from.x + (to.x - from.x) * progress,
                y: from.y + (to.y - from.y) * progress
            )
            let fade = sin(progress * .pi)
            context.fill(
                Path(ellipseIn: CGRect(x: point.x - 1.5, y: point.y - 1.5, width: 3, height: 3)),
                with: .color(AkzioColor.gold(0.55 * fade))
            )
        }
    }

    private func drawNodes(
        _ context: inout GraphicsContext,
        placed: [(node: UniverseNode, point: CGPoint)],
        time: Double
    ) {
        // Single slow breath, applied only to the current node.
        let phase = (time.truncatingRemainder(dividingBy: Motion.pulsePeriod)) / Motion.pulsePeriod
        let breath = canvas.runsAmbient ? 0.5 - 0.5 * cos(phase * 2 * .pi) : 0.35

        for entry in placed {
            let node = entry.node
            let radius = SignalUniverseLayout.radius(for: node)
            let rect = CGRect(
                x: entry.point.x - radius,
                y: entry.point.y - radius,
                width: radius * 2,
                height: radius * 2
            )
            if node.isCurrent {
                let bloom = radius + 10 + 4 * breath
                context.fill(
                    Path(ellipseIn: CGRect(
                        x: entry.point.x - bloom,
                        y: entry.point.y - bloom,
                        width: bloom * 2,
                        height: bloom * 2
                    )),
                    with: .radialGradient(
                        Gradient(colors: [AkzioColor.gold(0.34 - 0.16 * breath), .clear]),
                        center: entry.point,
                        startRadius: radius,
                        endRadius: bloom
                    )
                )
            }
            context.fill(Path(ellipseIn: rect), with: .color(nodeFill(node)))
            context.stroke(
                Path(ellipseIn: rect),
                with: .color(node.tone.color.opacity(node.status.isLive ? 0.95 : 0.45)),
                lineWidth: node.isCurrent ? 1.6 : 1
            )
            if node.id == selectedStageID {
                let ring = radius + 5
                context.stroke(
                    Path(ellipseIn: CGRect(
                        x: entry.point.x - ring,
                        y: entry.point.y - ring,
                        width: ring * 2,
                        height: ring * 2
                    )),
                    with: .color(AkzioColor.gold(0.7)),
                    lineWidth: 1
                )
            }
            if labelDensity != .minimal || node.isCurrent {
                var label = context.resolve(
                    Text(L10n.text(node.stage.displayName, language: language))
                        .font(AkzioFont.caption)
                        .foregroundColor(node.isCurrent ? AkzioColor.primaryText : AkzioColor.mutedText)
                )
                label.shading = .color(node.isCurrent ? AkzioColor.primaryText : AkzioColor.mutedText)
                context.draw(label, at: CGPoint(x: entry.point.x, y: entry.point.y + radius + 9))
            }
        }
    }

    private func nodeFill(_ node: UniverseNode) -> Color {
        switch node.status {
        case .running, .observing: AkzioColor.gold(0.85)
        case .succeeded, .completed: AkzioColor.gold(0.28)
        case .failed, .blocked, .rejected, .stale: AkzioColor.coral(0.55)
        case .notTriggered, .notApplicable, .skipped, .cancelled: AkzioColor.mutedText.opacity(0.25)
        default: AkzioColor.raisedSurface
        }
    }

    // MARK: Interactive overlay

    private func hitLayer(placed: [(node: UniverseNode, point: CGPoint)]) -> some View {
        ZStack {
            ForEach(placed, id: \.node.id) { entry in
                let radius = SignalUniverseLayout.radius(for: entry.node)
                Button {
                    onSelect(entry.node.id)
                    presentedStageID = presentedStageID == entry.node.id ? nil : entry.node.id
                } label: {
                    Circle()
                        .fill(Color.white.opacity(0.001))
                        .frame(width: radius * 2 + 14, height: radius * 2 + 14)
                }
                .buttonStyle(.plain)
                .position(entry.point)
                .help("\(entry.node.stage.displayName) — \(entry.node.status.style.label)")
                .accessibilityLabel(entry.node.stage.displayName)
                .accessibilityValue(entry.node.status.style.label)
                .accessibilityAddTraits(entry.node.id == selectedStageID ? [.isSelected] : [])
                .sharedElement(anchor(for: entry.node), in: anchorNamespace(for: entry.node))
            }
        }
    }

    @ViewBuilder
    private func stageOverlay(in size: CGSize) -> some View {
        if let stageID = presentedStageID,
           let node = workflow.node(id: stageID) {
            let panelSize = AkzioLayout.inspectorOverlaySize(in: size)
            ScrollView(.vertical) {
                StageInspectorPanel(
                    inspector: workflow.inspector(for: stageID),
                    node: node,
                    namespace: nil,
                    width: panelSize.width,
                    onDismiss: { presentedStageID = nil }
                )
            }
            .scrollIndicators(.never)
            .scrollBounceBehavior(.basedOnSize)
            .frame(
                width: panelSize.width,
                height: panelSize.height
            )
            .clipShape(RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous))
            .akzioShadow(.float)
            .padding(AkzioLayout.s3)
            .transition(.opacity.combined(with: .scale(scale: 0.97, anchor: .topTrailing)))
            .zIndex(10)
        }
    }

    /// Only nodes that have a counterpart on another page register an anchor.
    private func anchor(for node: UniverseNode) -> SharedElementID {
        if node.isCurrent { return .currentNode }
        switch node.stage {
        case .horizon(let horizon): return .horizonRing(horizon)
        case .learning: return .learningNode
        case .evaluate: return .evaluateNode
        default: return .noSharedElement
        }
    }

    private func anchorNamespace(for node: UniverseNode) -> Namespace.ID? {
        switch node.stage {
        case .horizon, .learning, .evaluate: namespace
        default: node.isCurrent ? namespace : nil
        }
    }
}
