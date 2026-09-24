import SwiftUI

// 文件导读：Archive 路由读取 Store 的 ArchivePresentation，页面 State 在当前已加载 rows
// 上执行搜索、筛选、排序、分页和表格/卡片切换；选择行由 Store 记忆，live 时可能异步读取
// 该 Run 的详情。先读 ArchiveQuery、body、filteredRows、selectedRow 与 select，区分本地展示筛选和 Observer 请求。
public enum ArchiveQuery {
    // 这些查询函数只在内存中的展示快照上工作，不代表对真实历史账本发起分页请求。
    public static func sorted(
        _ rows: [ArchiveRowPresentation],
        by key: RunSortKey,
        ascending: Bool
    ) -> [ArchiveRowPresentation] {
        rows.sorted { lhs, rhs in
            // 排序闭包先按用户选择的字段比较，无法比较时用稳定 ID 保证顺序可复现。
            let ordered: Bool?
            switch key {
            case .started:
                ordered = compare(lhs.startedAt, rhs.startedAt, ascending: ascending)
            case .duration:
                ordered = compare(lhs.durationSeconds, rhs.durationSeconds, ascending: ascending)
            case .result:
                ordered = compare(lhs.resultPpm, rhs.resultPpm, ascending: ascending)
            case .status:
                ordered = compare(lhs.status.displayName, rhs.status.displayName, ascending: ascending)
            }
            return ordered ?? (lhs.id < rhs.id)
        }
    }

    public static func page<T>(_ values: [T], number: Int, size: Int) -> [T] {
        // 页码和页大小先被夹在合法范围内，避免空数组或过大的页码造成越界。
        let size = max(1, size)
        let page = min(max(1, number), pageCount(total: values.count, size: size))
        let start = (page - 1) * size
        return Array(values[start..<min(start + size, values.count)])
    }

    public static func pageCount(total: Int, size: Int) -> Int {
        // 空集合仍给出一页，供界面保持合法的“第 1/1 页”状态。
        max(1, Int(ceil(Double(total) / Double(max(1, size)))))
    }

    private static func compare<T: Comparable>(
        _ lhs: T?,
        _ rhs: T?,
        ascending: Bool
    ) -> Bool? {
        // T: Comparable 要求两侧有值时可比较；nil 排在有值之后，两个 nil 返回 nil 交给稳定 ID 破同序。
        switch (lhs, rhs) {
        case (nil, nil): nil
        case (nil, _): false
        case (_, nil): true
        case let (lhs?, rhs?) where lhs == rhs: nil
        case let (lhs?, rhs?): ascending ? lhs < rhs : lhs > rhs
        }
    }
}

// MARK: - Run archive
//
// The run ledger. Filtering, sorting, density and pagination are all view state:
// the page never mutates the snapshot, it only chooses how to read it. Selecting a
// 选中行打开预览；详情按钮由父页把该行快照封装为只读独立窗口 payload。
struct RunArchivePage: View {
    let store: ObservatoryStore

    // Environment 提供共享动画、语言和新窗口入口；筛选/排序/分页均是本页 State。
    @Environment(\.sharedNamespace) private var namespace
    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    @Environment(\.openWindow) private var openWindow

    @State private var query = ""
    @State private var purposeFilter: RunPurpose?
    @State private var statusFilter: WorkflowStatus?
    @State private var sortKey: RunSortKey = .started
    @State private var ascending = false
    @State private var layout: ArchiveLayout = .table
    @State private var page = 1
    @State private var detailPayload: DetachedRunPayload?

    // live/mock 来源由 Store.displayArchive 统一选择；本页不直接解码 Observer payload。
    private var archive: ArchivePresentation { store.displayArchive }

    var body: some View {
        // 页面把 Store 的 archive 投影读成表格或卡片；筛选变化只重置本页页码。
        PageScaffold(route: .runArchive) {
            VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                StagedSection(index: 0) {
                    ArchiveFilterBar(
                        query: $query,
                        purpose: $purposeFilter,
                        status: $statusFilter,
                    density: densityBinding,
                    activeFilters: activeFilters,
                    resultLabel: resultLabel
                )
                .onChange(of: query) { _, _ in page = 1 }
                .onChange(of: purposeFilter) { _, _ in page = 1 }
                .onChange(of: statusFilter) { _, _ in page = 1 }
                }
                HStack(alignment: .top, spacing: AkzioLayout.s2) {
                    StagedSection(index: 1) {
                        ledger
                    }
                    if let row = selectedRow {
                        // 预览回调捕获当前 row、Store 和语言，把详情或窗口动作交回父页。
                        CollapsibleInspector(
                            title: "Run Preview",
                            symbol: "sidebar.trailing",
                            width: AkzioLayout.inspectorWidth
                        ) {
                    RunPreviewPanel(
                        row: row,
                        stageProgress: store.selectedArchiveStageProgress,
                        outcomeEvidence: store.selectedArchiveOutcomeEvidence,
                                onViewDetails: { detailPayload = DetachedRunPayload(row, stages: store.selectedArchiveStageProgress, outcomeEvidence: store.selectedArchiveOutcomeEvidence, language: language) },
                                onOpenInNewWindow: {
                                    openWindow(value: DetachedRunPayload(row, stages: store.selectedArchiveStageProgress, outcomeEvidence: store.selectedArchiveOutcomeEvidence, language: language))
                                },
                                onDismiss: { store.selectedArchiveRowID = nil }
                            )
                        }
                        .transition(
                            .move(edge: .trailing).combined(with: .opacity)
                        )
                    }
                }
                .animation(policy.resolve(Motion.panel), value: selectedRow?.id)
                StagedSection(index: 2) {
                    paginationBar
                }
            }
        } toolbar: {
            toolbar
        }
        .sheet(item: $detailPayload) { payload in
            DetachedRunWindow(payload: payload)
        }
    }

    // MARK: Ledger

    private var ledger: some View {
        // ledger 根据本地 layout State 选择表格或卡片，两种呈现共享同一份 visibleRows。
        SectionCard(title: "Runs", subtitle: statsSubtitle) {
            Group {
                switch layout {
                case .table:
                    RunTable(
                        rows: visibleRows,
                        selectedID: store.selectedArchiveRowID,
                        rowHeight: rowHeight,
                        sortKey: $sortKey,
                        ascending: $ascending,
                        namespace: namespace,
                        onSelect: select
                    )
                case .cards:
                    cardGrid
                }
            }
            .frame(minHeight: 360, alignment: .top)
            .overlay(alignment: .center) {
                if visibleRows.isEmpty {
                    emptyState
                }
            }
        }
    }

    private var cardGrid: some View {
        // 每张卡的选择闭包只写入选中的行 ID，不直接修改 archive 快照。
        PageScroll {
            LazyVGrid(
                columns: [GridItem(.adaptive(minimum: 236), spacing: AkzioLayout.s3)],
                spacing: AkzioLayout.s3
            ) {
                ForEach(visibleRows) { row in
                    Button { select(row.id) } label: {
                        runCard(row)
                    }
                    .buttonStyle(PressableButtonStyle(scale: 0.985))
                }
            }
            .padding(.top, 2)
        }
        .animation(policy.resolve(Motion.cardDetail), value: visibleRows.map(\.id))
    }

    private func runCard(_ row: ArchiveRowPresentation) -> some View {
        // 卡片状态由传入行和 Store 当前选择派生，按钮闭包捕获该行的稳定 ID。
        let isSelected = row.id == store.selectedArchiveRowID
        return VStack(alignment: .leading, spacing: AkzioLayout.s2) {
            HStack(spacing: AkzioLayout.s2) {
                Text(String(row.runID.prefix(8))).akzioMono(12, color: AkzioColor.primaryText)
                Spacer(minLength: 4)
                StatusBadge(row.status.status, size: .compact)
            }
            PillTag(row.purposeLabel, tone: row.purpose.tone)
            Text(row.topology).akzioMono(10, color: AkzioColor.secondaryText).lineLimit(1)
            HStack(spacing: AkzioLayout.s2) {
                Text(L10n.text(PpmFormatter.percent(ppm: row.resultPpm), language: language))
                    .akzioMono(13, color: resultColor(row))
                Spacer(minLength: 4)
                Text(L10n.text(PpmFormatter.duration(seconds: row.durationSeconds), language: language))
                    .akzioMono(10, color: AkzioColor.mutedText)
            }
        }
        .padding(AkzioLayout.s3)
        .frame(maxWidth: .infinity, alignment: .leading)
        .akzioGlassBackdrop(AkzioColor.raisedSurface, radius: AkzioLayout.cardRadius)
        .overlay(
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(isSelected ? AkzioColor.goldHairline : AkzioColor.hairline, lineWidth: 1)
        )
        .hoverLift()
        .animation(policy.resolve(Motion.selection), value: isSelected)
    }

    private var emptyArchiveMessage: String {
        // 筛选后为空与加载源不可用分别提示；连接状态不被解释成服务端没有历史记录。
        if !archive.rows.isEmpty { return L10n.text("No runs match filters", language: language) }
        switch store.observerState {
        case .offline: return "尚未连接 Core · 历史运行尚未载入"
        case .connecting: return "正在连接 Core · 等待历史运行"
        case .stale: return "连接已中断 · 暂无可显示的历史快照"
        case .connected, .mock: return "暂无历史运行"
        }
    }

    private var emptyState: some View {
        // 空态区分“没有历史数据”和“筛选后无匹配”，清除闭包只恢复筛选 State。
        VStack(spacing: AkzioLayout.s2) {
            Image(systemName: "line.3.horizontal.decrease.circle")
                .font(.system(size: 20, weight: .light))
                .foregroundStyle(AkzioColor.mutedText)
            Text(emptyArchiveMessage)
                .akzioText(.body, color: AkzioColor.secondaryText)
            if !query.isEmpty || purposeFilter != nil || statusFilter != nil {
            Button(L10n.text("Clear Filters", language: language)) {
                withAnimation(policy.resolve(Motion.control)) {
                    query = ""
                    purposeFilter = nil
                    statusFilter = nil
                }
            }
            .buttonStyle(PressableButtonStyle())
            .akzioText(.label, color: AkzioColor.primaryGold)
            }
        }
        .materialize(isVisible: visibleRows.isEmpty, policy: policy)
    }

    // MARK: Toolbar and pagination

    private var toolbar: some View {
        // 工具栏只修改 layout Binding；统计文本来自当前 archive 投影，不重新读取 Store。
        HStack(spacing: AkzioLayout.s2) {
            AkzioSegmentedControl(
                selection: $layout,
                options: ArchiveLayout.allCases.map { (value: $0, label: $0.displayName) }
            )
            Text(archive.successRatePpm.map { PpmFormatter.share(ppm: $0) } ?? L10n.text("No data", language: language))
                .akzioMono(11, color: AkzioColor.primaryGold)
                .akzioNumeric(archive.successRatePpm, policy: policy)
            Text(L10n.text("Workflow completion rate", language: language)).akzioText(.caption)
        }
    }

    private var paginationBar: some View {
        // 分页按钮闭包只递增或递减页码，enabled 条件保证动作不会越界。
        HStack(spacing: AkzioLayout.s2) {
            Text(rangeLabel).akzioMono(10, color: AkzioColor.mutedText)
            Spacer(minLength: AkzioLayout.s3)
            pageButton("chevron.left", enabled: page > 1) { page -= 1 }
            Text("\(L10n.text("Page", language: language)) \(page) / \(totalPages)")
                .akzioMono(10, color: AkzioColor.secondaryText)
                .akzioNumeric(Double(page), policy: policy)
            pageButton("chevron.right", enabled: page < totalPages) { page += 1 }
            Text(L10n.text(store.settings.density.displayName, language: language)).akzioText(.caption)
        }
        .akzioCard(padding: AkzioLayout.s2)
        .animation(policy.resolve(Motion.control), value: page)
    }

    private func pageButton(
        _ symbol: String,
        enabled: Bool,
        action: @escaping () -> Void
    ) -> some View {
        // action 是父页提供的无参闭包；本组件只负责把它包成可禁用的按钮。
        Button(action: action) {
            Image(systemName: symbol)
                .font(.system(size: 9, weight: .bold))
                .foregroundStyle(enabled ? AkzioColor.primaryText : AkzioColor.mutedText)
                .frame(width: 22, height: 20)
                .contentShape(Rectangle())
        }
        .buttonStyle(PressableButtonStyle())
        .disabled(!enabled)
    }

    // MARK: Derived state

    /// Sort and filter are applied to the loaded page only — pagination here is a
    /// visual mock over a fixture page, not a query against a real ledger.
    private var filteredRows: [ArchiveRowPresentation] {
        // filter 闭包组合三个纯匹配函数，结果随后按 State 指定字段排序。
        let filtered = archive.rows.filter { row in
            matchesQuery(row) && matchesPurpose(row) && matchesStatus(row)
        }
        return ArchiveQuery.sorted(filtered, by: sortKey, ascending: ascending)
    }

    private var visibleRows: [ArchiveRowPresentation] {
        // visibleRows 是当前页的派生数组，页面不会因翻页改变底层 archive.rows。
        ArchiveQuery.page(filteredRows, number: page, size: archive.pageSize)
    }

    private func matchesQuery(_ row: ArchiveRowPresentation) -> Bool {
        // 搜索只匹配已投影的四个字符串字段，转小写后做子串包含，不查询详情正文。
        guard !query.isEmpty else { return true }
        let needle = query.lowercased()
        return row.runID.lowercased().contains(needle)
            || row.topology.lowercased().contains(needle)
            || row.model.lowercased().contains(needle)
            || row.currentStage.lowercased().contains(needle)
    }

    private func matchesPurpose(_ row: ArchiveRowPresentation) -> Bool {
        // nil 表示不按目的缩小结果；有值时要求 RunPurpose 完全相等。
        purposeFilter.map { $0 == row.purpose } ?? true
    }

    private func matchesStatus(_ row: ArchiveRowPresentation) -> Bool {
        // nil 保留所有状态；有值时只匹配对应 WorkflowStatus。
        statusFilter.map { $0 == row.status } ?? true
    }

    private var selectedRow: ArchiveRowPresentation? {
        // 仅当 Store 的选择仍存在于当前可见页时显示预览，筛选变化不会显示过期行。
        guard let id = store.selectedArchiveRowID else { return nil }
        return visibleRows.first { $0.id == id }
    }

    private var activeFilters: [String] {
        // 既有投影标签与本页输入合并后只用于显示 Chips，不参与 matches* 判定。
        var filters = archive.activeFilters
        if !query.isEmpty { filters.append("\(L10n.text("Search", language: language)): \(query)") }
        if let purposeFilter { filters.append(purposeFilter.displayName) }
        if let statusFilter { filters.append(statusFilter.displayName) }
        return filters
    }

    private var resultLabel: String {
        // shown 是当前页数量，matching 是筛选后总量；二者都只覆盖当前加载的 projection。
        "\(PpmFormatter.count(visibleRows.count)) \(L10n.text("shown", language: language)) · \(PpmFormatter.count(filteredRows.count)) \(L10n.text("matching", language: language))"
    }

    private var statsSubtitle: String {
        // successRatePpm 缺失时显示 No data，不把没有终态 Run 解释为 0%。
        "\(L10n.text("Workflow completion rate", language: language)) \(archive.successRatePpm.map { PpmFormatter.share(ppm: $0) } ?? L10n.text("No data", language: language)) · \(L10n.text("sorted by", language: language)) \(L10n.text(sortKey.title, language: language))"
    }

    private var totalPages: Int {
        // ArchiveQuery 保证至少一页，因此空结果也能表示为第 1/1 页。
        ArchiveQuery.pageCount(total: filteredRows.count, size: archive.pageSize)
    }

    private var rangeLabel: String {
        // 空集单独显示 0 runs；非空时按一基页码计算当前投影内的行号范围。
        guard !filteredRows.isEmpty else { return "0 \(L10n.text("runs", language: language))" }
        let start = (page - 1) * archive.pageSize + 1
        let end = min(start + visibleRows.count - 1, filteredRows.count)
        return "\(start)–\(end) \(L10n.text("of", language: language)) \(PpmFormatter.count(filteredRows.count)) \(L10n.text("runs", language: language))"
    }

    private var rowHeight: CGFloat {
        // density.scale 只缩放行的视觉高度，不改变行数或任务阶段进度。
        (28 * store.settings.density.scale).rounded()
    }

    private var densityBinding: Binding<SettingsPresentation.Density> {
        // Binding 的 get/set 闭包把 ArchiveFilterBar 的密度选择映射到 Store 设置。
        Binding(
            get: { store.settings.density },
            set: { store.settings.density = $0 }
        )
    }

    private func select(_ id: String) {
        // 选择动作由父页统一包裹面板动画；Store 可能读取 live 详情，但不触发运行或订单。
        withAnimation(policy.resolve(Motion.panel)) {
            store.selectArchiveRun(id)
        }
    }

    private func resultColor(_ row: ArchiveRowPresentation) -> Color {
        // resultPpm 是行投影中的可选结果指标；nil 保持中性色，有值才按正负映射颜色。
        guard let result = row.resultPpm else { return AkzioColor.mutedText }
        return result >= 0 ? AkzioColor.primaryGold : AkzioColor.actionCoral
    }
}

// MARK: - Layout switch

enum ArchiveLayout: String, CaseIterable, Hashable {
    case table, cards

    var displayName: String {
        // 布局名称仅供 segmented control 显示，不影响 rows 或筛选规则。
        switch self {
        case .table: "Table"
        case .cards: "Cards"
        }
    }
}
