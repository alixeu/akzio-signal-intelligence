import SwiftUI

// MARK: - Workflow
//
// Progress strip, the DAG itself, and the Stage Inspector on the right. The canvas
// owns zoom/pan state; the toolbar only mutates that transform.
struct WorkflowPage: View {
    // store 是页面唯一的 Rust/Core projection 入口；本页的 State 只保存画布视图偏好，
    // 不复制 workflow 的 durable 状态。
    let store: ObservatoryStore

    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.appLanguage) private var language
    @State private var scale: CGFloat = 1
    @State private var offset: CGSize = .zero
    @State private var showsLabels = true
    @State private var showsParticles = true
    @State private var showsGrid = false
    @State private var highlightsCriticalPath = true
    @State private var collapsesOptional = false

    // computed property 每次读取最新 displayWorkflow；它不是本地缓存，也不绕过 Store 读取
    // 原始事件或 SQLite。
    private var workflow: WorkflowPresentation { store.displayWorkflow }

    var body: some View {
        // Debug 模式展示隔离 Core 的专用检查面板；普通模式才展示进度、DAG、Stage Inspector
        // 和仅影响画布的 toolbar。两条分支都只消费 Store projection。
        PageScaffold(route: .workflow) {
            if store.debugEnabled {
                ScrollView {
                    DebugWorkflowPanel(store: store)
                        .frame(maxWidth: .infinity, alignment: .topLeading)
                }
            } else {
            PageScroll {
            VStack(alignment: .leading, spacing: AkzioLayout.s4) {
                RuntimeInspectorPanel(store: store)
                if let summary = store.workflowEvidenceSummary {
                    Text(summary).akzioText(.body).textSelection(.enabled)
                }
                StagedSection(index: 0) {
                    WorkflowProgressStrip(workflow: workflow, namespace: namespace)
                }
                HStack(alignment: .top, spacing: AkzioLayout.s4) {
                    StagedSection(index: 1) {
                        graph
                    }
                    .frame(maxWidth: .infinity)
                    .layoutPriority(1)
                    StagedSection(index: 2) {
                CollapsibleInspector(
                    title: "Stage Inspector",
                    symbol: "list.bullet.rectangle",
                    width: AkzioLayout.workflowInspectorWidth
                ) {
                    ScrollView {
                    StageInspectorPanel(
                        inspector: store.selectedStageInspector,
                        node: store.activeStage,
                        namespace: namespace,
                        width: AkzioLayout.workflowInspectorWidth
                    )
                    }.frame(height: 520)
                        }
                    }
                }
            }
            }
            }
        } toolbar: {
            if !store.debugEnabled {
            CanvasToolbar(
                scale: $scale,
                offset: $offset,
                showsLabels: $showsLabels,
                showsParticles: $showsParticles,
                showsGrid: $showsGrid,
                highlightsCriticalPath: $highlightsCriticalPath,
                collapsesOptional: $collapsesOptional
            )
            }
        }
    }

    private var graph: some View {
        // graph 把 WorkflowPresentation 的节点/边交给 Canvas，把 selection closure 交回
        // Store；标题和 legend 仍是展示投影，不会改变 Rust workflow。
        SectionCard(
            title: "Pipeline",
            subtitle: workflow.activeStageID.flatMap { workflow.node(id: $0)?.stage.displayName }
                ?? store.displayRun.status.displayName
        ) {
            WorkflowDagCanvas(
                workflow: visibleWorkflow,
                selectedStageID: store.selectedStageID,
                namespace: namespace,
                showsLabels: showsLabels,
                showsParticles: showsParticles,
                highlightsCriticalPath: highlightsCriticalPath,
                scale: $scale,
                offset: $offset,
                onSelect: { store.selectedStageID = $0 }
            )
            .frame(height: 430)
            .background {
                if showsGrid {
                    GridBackdrop()
                }
            }
        } accessory: {
            HStack(spacing: AkzioLayout.s3) {
                ForEach(WorkflowEdgeKind.allCases, id: \.self) { kind in
                    HStack(spacing: 4) {
                        Rectangle()
                            .fill(kind.tone.color.opacity(0.8))
                            .frame(width: 12, height: 1.4)
                    Text(L10n.text(kind.displayName, language: language)).akzioText(.caption)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity)
    }

    /// "Collapse Optional" is a view control: it hides the Critic branch without
    /// pretending the stage does not exist in the plan.
    private var visibleWorkflow: WorkflowPresentation {
        // 过滤 optional 节点后同步过滤两端都不存在的边，并重新验证 activeStageID；inspector
        // 内容与交易日统计保持原 projection，隐藏是视图变换而不是 DAG 删除。
        guard collapsesOptional else { return workflow }
        let nodes = workflow.nodes.filter { !$0.stage.isOptional }
        let ids = Set(nodes.map(\.id))
        let edges = workflow.edges.filter { ids.contains($0.from) && ids.contains($0.to) }
        return WorkflowPresentation(
            nodes: nodes,
            edges: edges,
            activeStageID: ids.contains(workflow.activeStageID ?? "") ? workflow.activeStageID : nil,
            inspector: workflow.inspector,
            observedTradingDays: workflow.observedTradingDays,
            totalTradingDays: workflow.totalTradingDays,
            stageInspectors: workflow.stageInspectors.filter { ids.contains($0.key) }
        )
    }
}

// MARK: - Grid

/// Faint 24pt grid, off by default. Drawn once; it never animates.
private struct GridBackdrop: View {
    var body: some View {
        // Canvas 只画固定 24pt 低对比网格并关闭 hit testing；它不参与 DAG layout，也没有
        // 任何状态或 Core 请求。
        Canvas { context, size in
            var path = Path()
            for x in stride(from: 0, through: size.width, by: 24) {
                path.move(to: CGPoint(x: x, y: 0))
                path.addLine(to: CGPoint(x: x, y: size.height))
            }
            for y in stride(from: 0, through: size.height, by: 24) {
                path.move(to: CGPoint(x: 0, y: y))
                path.addLine(to: CGPoint(x: size.width, y: y))
            }
            context.stroke(path, with: .color(.white.opacity(0.03)), lineWidth: 0.5)
        }
        .allowsHitTesting(false)
    }
}
