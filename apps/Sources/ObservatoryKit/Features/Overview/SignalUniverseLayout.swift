import SwiftUI

// MARK: - Signal Universe geometry
//
// Three elliptical orbits carry the twelve stage kinds. Positions are pure
// functions of the snapshot, so the canvas and the interactive overlay agree and a
// screenshot is reproducible.
struct UniverseNode: Identifiable, Hashable {
    // UniverseNode 是 WorkflowNodePresentation 的几何投影，保留状态和轨道信息供 Canvas 使用。
    let id: String
    let stage: WorkflowStageKind
    let status: AkzioStatus
    let orbit: Int
    /// Slot angle in radians, measured from the positive x axis.
    let angle: Double
    let isCurrent: Bool

    var tone: AkzioTone { status.style.tone }
}

enum SignalUniverseLayout {
    // 所有函数都是纯几何计算；Canvas 和命中层必须使用同一套结果。
    /// Radii as fractions of the card's own width and height. Scaling each axis
    /// independently is what lets the field fill a wide card instead of leaving a
    /// dead band above and below a circle inscribed in the shorter side.
    static let orbits: [(rx: CGFloat, ry: CGFloat)] = [
        (0.18, 0.17),
        (0.29, 0.275),
        (0.40, 0.375),
    ]

    /// Half-extent of an orbit in view coordinates.
    static func extent(_ orbit: (rx: CGFloat, ry: CGFloat), in size: CGSize) -> CGSize {
        CGSize(width: orbit.rx * size.width, height: orbit.ry * size.height)
    }

    static func orbit(for stage: WorkflowStageKind) -> Int {
        switch stage {
        case .planner, .evidenceGate, .synthesizer, .decisionGate: 0
        case .analyst, .critic, .executionGate, .paperCommit: 1
        case .reconcile, .evaluate, .horizon, .learning: 2
        }
    }

    static func nodes(from workflow: WorkflowPresentation) -> [UniverseNode] {
        // group/map 闭包按阶段轨道重新组织 workflow 节点，并固定每个节点的槽位角度。
        let grouped = Dictionary(grouping: workflow.nodes) { orbit(for: $0.stage) }
        var result: [UniverseNode] = []
        for orbitIndex in 0..<orbits.count {
            let members = grouped[orbitIndex] ?? []
            let count = max(members.count, 1)
            // Each orbit starts at a different phase so slots never line up in a row.
            let phase = Double(orbitIndex) * 0.42
            for (slot, node) in members.enumerated() {
                result.append(
                    UniverseNode(
                        id: node.id,
                        stage: node.stage,
                        status: node.status,
                        orbit: orbitIndex,
                        angle: phase + Double(slot) / Double(count) * 2 * .pi,
                        isCurrent: node.id == workflow.activeStageID
                    )
                )
            }
        }
        return result
    }

    /// `rotation` is the ambient drift in radians; 0 renders the canonical frame.
    static func position(_ node: UniverseNode, in size: CGSize, rotation: Double) -> CGPoint {
        // 位置由节点固有角度、轨道半径和当前环境旋转共同决定；rotation 为零即稳定基准帧。
        let orbit = orbits[min(node.orbit, orbits.count - 1)]
        let half = extent(orbit, in: size)
        // Outer orbits drift slower, like a real system.
        let drift = rotation / Double(node.orbit + 1)
        let angle = node.angle + drift
        let x: CGFloat = size.width / 2 + CGFloat(cos(angle)) * half.width
        let y: CGFloat = size.height / 2 + CGFloat(sin(angle)) * half.height
        return CGPoint(x: x, y: y)
    }

    static func radius(for node: UniverseNode) -> CGFloat {
        switch node.stage {
        case .planner, .synthesizer: 9
        case .decisionGate, .executionGate, .evidenceGate: 8
        case .horizon: 7
        default: 7.5
        }
    }

    /// Edges drawn in the field: the pipeline order, plus the three horizon spurs.
    static func edges(from workflow: WorkflowPresentation) -> [(String, String)] {
        // filter/map 闭包只抽取非冲突边；SignalUniverseCanvas 会按完整边模型决定样式。
        workflow.edges
            .filter { $0.kind != .conflict }
            .map { ($0.from, $0.to) }
    }

    /// Ambient drift in radians for a given time, or 0 when motion is paused.
    static func rotation(time: Double, policy: CanvasRenderPolicy) -> Double {
        // 动效策略暂停时返回零，使离屏截图和减少动效场景保持确定性。
        guard policy.runsAmbient else { return 0 }
        return time / Motion.ambientPeriod * 2 * .pi * 0.08
    }
}
