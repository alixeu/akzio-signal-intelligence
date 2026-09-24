import SwiftUI

// MARK: - DAG layout
//
// Layered layout: column from the pipeline stage, row from the parallel slot.
// The result is cached per node signature because the geometry only changes when
// the plan changes — not when a status does, and not while panning.
struct DagLayout: Sendable {
    // layout 是由节点 id/column/row 生成的值快照；Canvas 读取 positions，Rust projection
    // 的状态变化不会被这里重新解释为新的拓扑。
    let positions: [String: CGPoint]
    let contentSize: CGSize

    static let columnSpacing: CGFloat = 126
    static let rowSpacing: CGFloat = 112
    static let margin: CGFloat = 56
    func point(_ id: String) -> CGPoint? { positions[id] }

    // 空图或非法尺寸返回 1；正常图先按 viewport 扣除 24pt 内边距计算 fit，再限制不超过
    // 1，避免小图被放大到破坏设计密度。
    /// Scale that fits the whole graph into `size`, capped at 1 so a small graph is
    /// never blown up past its designed density.
    func fitScale(in size: CGSize) -> CGFloat {
        guard contentSize.width > 0, contentSize.height > 0 else { return 1 }
        let fit = min(
            max(1, size.width - 24) / contentSize.width,
            max(1, size.height - 24) / contentSize.height
        )
        return min(1, fit)
    }

    static func layout(for nodes: [WorkflowNodePresentation]) -> DagLayout {
        // signature 只包含 id、column、row，所以 status、选中态、动画时间或平移不会使
        // 几何重排；同一拓扑可复用缓存布局。
        let signature = nodes.map { "\($0.id):\($0.column).\($0.row)" }.joined(separator: "|")
        if let cached = Cache.read(signature) { return cached }

        var positions: [String: CGPoint] = [:]
        let columns = Dictionary(grouping: nodes, by: \.column)
        let tallest = max(1, columns.values.map(\.count).max() ?? 1)

        // 每列内部先按语义 row、再按 id 排序；leadingSlack 把较短列垂直居中，row 只是
        // 排序提示，不是固定屏幕坐标。
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
        // Cache 是进程内的 best-effort memo：nonisolated(unsafe) 明确绕过 actor 隔离，
        // 不持久化、不承担并发同步或业务权威；超过 32 个 signature 时整体清空。
        Cache.write(signature, layout)
        return layout
    }

    /// Tiny memo so panning and status ticks never recompute geometry.
    private enum Cache {
        // 该静态字典只服务同一进程的画布重绘；read/write 都是同步直接访问，没有 Store 或
        // Core journal 参与，因此缓存命中不能证明 Rust topology 已改变或已完成。
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
