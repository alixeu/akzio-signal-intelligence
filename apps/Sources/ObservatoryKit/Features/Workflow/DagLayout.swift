import SwiftUI

// MARK: - DAG layout
//
// Layered layout: column from the pipeline stage, row from the parallel slot.
// The result is cached per node signature because the geometry only changes when
// the plan changes — not when a status does, and not while panning.
struct DagLayout: Sendable {
    let positions: [String: CGPoint]
    let contentSize: CGSize

    static let columnSpacing: CGFloat = 126
    static let rowSpacing: CGFloat = 112
    static let margin: CGFloat = 56
    /// Never shrink past this: below it the stage labels stop being readable, and a
    /// pannable graph is better than an illegible one.
    static let minimumFitScale: CGFloat = 0.68

    func point(_ id: String) -> CGPoint? { positions[id] }

    /// Scale that fits the whole graph into `size`, capped at 1 so a small graph is
    /// never blown up past its designed density.
    func fitScale(in size: CGSize) -> CGFloat {
        guard contentSize.width > 0, contentSize.height > 0 else { return 1 }
        let fit = min(
            max(1, size.width - 24) / contentSize.width,
            max(1, size.height - 24) / contentSize.height
        )
        return min(1, max(Self.minimumFitScale, fit))
    }

    static func layout(for nodes: [WorkflowNodePresentation]) -> DagLayout {
        let signature = nodes.map { "\($0.id):\($0.column).\($0.row)" }.joined(separator: "|")
        if let cached = Cache.read(signature) { return cached }

        var positions: [String: CGPoint] = [:]
        let columns = Dictionary(grouping: nodes, by: \.column)
        let tallest = max(1, columns.values.map(\.count).max() ?? 1)

        // Rows in WorkflowLayout are semantic ordering hints, not absolute slots.
        // Ranking the populated nodes first prevents a lone row-1 node from being
        // shifted below the centre while three parallel Analysts stay centred.
        for (column, group) in columns {
            let ordered = group.sorted {
                $0.row == $1.row ? $0.id < $1.id : $0.row < $1.row
            }
            let leadingSlack = CGFloat(tallest - ordered.count) / 2
            for (index, node) in ordered.enumerated() {
                let x = margin + CGFloat(column) * columnSpacing
                let y = margin + (leadingSlack + CGFloat(index)) * rowSpacing
                positions[node.id] = CGPoint(x: x, y: y)
            }
        }

        let lastColumn = columns.keys.max() ?? 0
        let layout = DagLayout(
            positions: positions,
            contentSize: CGSize(
                width: margin * 2 + CGFloat(lastColumn) * columnSpacing,
                height: margin * 2 + CGFloat(tallest - 1) * rowSpacing + 28
            )
        )
        Cache.write(signature, layout)
        return layout
    }

    /// Tiny memo so panning and status ticks never recompute geometry.
    private enum Cache {
        nonisolated(unsafe) private static var storage: [String: DagLayout] = [:]

        static func read(_ key: String) -> DagLayout? { storage[key] }

        static func write(_ key: String, _ value: DagLayout) {
            if storage.count > 32 { storage.removeAll() }
            storage[key] = value
        }
    }
}

// MARK: - Node radius

extension DagLayout {
    static func radius(for node: WorkflowNodePresentation) -> CGFloat {
        switch node.stage {
        case .decisionGate, .executionGate, .evidenceGate: 22
        case .planner, .synthesizer: 21
        case .horizon: 17
        default: 19
        }
    }
}
