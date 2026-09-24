import Foundation

// MARK: - Portfolio

public struct EquityPoint: Sendable, Hashable, Identifiable {
    // EquityPoint 是曲线的不可变采样点；timestamp 可缺失时 chartX 回退到稳定 index。
    public let index: Int
    public let minutesFromOpen: Int
    public let timestamp: Date?
    public let portfolio: Double
    public let benchmark: Double?

    public var id: Int { index }

    public init(
        index: Int,
        minutesFromOpen: Int,
        timestamp: Date? = nil,
        portfolio: Double,
        benchmark: Double?
    ) {
        // 初始化保留 portfolio/benchmark Optional 差异，不把缺失 benchmark 填成零。
        self.index = index
        self.minutesFromOpen = minutesFromOpen
        self.timestamp = timestamp
        self.portfolio = portfolio
        self.benchmark = benchmark
    }

    /// Clock label derived from the frozen anchor, never from `Date()`.
    public var timeLabel: String {
        // timeLabel 由开盘分钟偏移计算，适合没有 timestamp 的离线或 fixture 点。
        let totalMinutes = 9 * 60 + 30 + minutesFromOpen
        return String(format: "%02d:%02d", totalMinutes / 60, totalMinutes % 60)
    }

    public var chartX: Double {
        // 有真实时间戳时使用时间轴；否则使用 index，保证图表仍可排序和绘制。
        timestamp?.timeIntervalSinceReferenceDate ?? Double(index)
    }

    public func axisLabel(for range: EquityRange, locale: Locale) -> String {
        // axisLabel 只在读取时按范围选择日期模板，不改变采样点本身。
        guard let timestamp else { return timeLabel }
        let formatter = DateFormatter()
        formatter.locale = locale
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(identifier: "America/New_York")
        let template = switch range {
        case .oneDay: "Hm"
        case .fiveDay: "EEEMd"
        default: "Md"
        }
        formatter.setLocalizedDateFormatFromTemplate(template)
        return formatter.string(from: timestamp)
    }
}

public enum EquityRange: String, CaseIterable, Sendable, Identifiable {
    // rawValue 是页面选择器显示的范围 token，pointCount 是 fixture/图表的展示规模。
    case oneDay = "1D"
    case fiveDay = "5D"
    case oneMonth = "1M"
    case threeMonth = "3M"
    case ytd = "YTD"
    case oneYear = "1Y"
    case all = "All"

    public var id: String { rawValue }
    public var pointCount: Int {
        switch self {
        case .oneDay: 78
        case .fiveDay: 100
        case .oneMonth: 120
        case .threeMonth: 140
        case .ytd: 160
        case .oneYear: 180
        case .all: 200
        }
    }
}

public struct AllocationRow: Sendable, Hashable, Identifiable {
    // allocation row 同时保存 actual/target，delta 与 overweight 都由值派生。
    public let label: String
    public let actualPpm: Int
    public let targetPpm: Int

    public var id: String { label }

    public init(label: String, actualPpm: Int, targetPpm: Int) {
        self.label = label
        self.actualPpm = actualPpm
        self.targetPpm = targetPpm
    }

    // delta/isOverweight 是页面读取边界的计算属性，不会修改权重字段。
    public var deltaPpm: Int { actualPpm - targetPpm }
    public var isOverweight: Bool { deltaPpm > 0 }
}

public struct PositionPresentation: Sendable, Hashable, Identifiable {
    // PositionPresentation 是资产 allocation 到 Portfolio 卡片的值语义投影，P&L 可缺失。
    public let asset: TradableAsset
    public let weightPpm: Int
    public let marketValueMicros: Int64
    public let pnlMicros: Int64?
    public let pnlPpm: Int?
    public let spark: [Double]
    public let actualPpm: Int
    public let targetPpm: Int

    public var id: String { asset.rawValue }

    public init(
        asset: TradableAsset,
        weightPpm: Int,
        marketValueMicros: Int64,
        pnlMicros: Int64?,
        pnlPpm: Int?,
        spark: [Double],
        actualPpm: Int,
        targetPpm: Int
    ) {
        self.asset = asset
        self.weightPpm = weightPpm
        self.marketValueMicros = marketValueMicros
        self.pnlMicros = pnlMicros
        self.pnlPpm = pnlPpm
        self.spark = spark
        self.actualPpm = actualPpm
        self.targetPpm = targetPpm
    }

    public var isGain: Bool { pnlMicros.map { $0 >= 0 } ?? true }
}

public struct OrderPresentation: Sendable, Hashable, Identifiable {
    // 订单模型保留资产、方向、数量、限价和回执状态，页面不从状态推断成交。
    public let id: String
    public let timeLabel: String
    public let asset: TradableAsset
    public let side: OrderSide
    public let type: String
    public let quantityMicros: Int64
    public let limitPriceMicros: Int64?
    public let state: OrderReceiptState

    public init(
        id: String,
        timeLabel: String,
        asset: TradableAsset,
        side: OrderSide,
        type: String,
        quantityMicros: Int64,
        limitPriceMicros: Int64?,
        state: OrderReceiptState
    ) {
        self.id = id
        self.timeLabel = timeLabel
        self.asset = asset
        self.side = side
        self.type = type
        self.quantityMicros = quantityMicros
        self.limitPriceMicros = limitPriceMicros
        self.state = state
    }
}

public struct FillPresentation: Sendable, Hashable, Identifiable {
    // FillPresentation 只表示已由上游确认的成交记录，venue 和数量保持可追溯。
    public let id: String
    public let timeLabel: String
    public let asset: TradableAsset
    public let side: OrderSide
    public let quantityMicros: Int64
    public let priceMicros: Int64
    public let venue: String

    public init(
        id: String,
        timeLabel: String,
        asset: TradableAsset,
        side: OrderSide,
        quantityMicros: Int64,
        priceMicros: Int64,
        venue: String
    ) {
        self.id = id
        self.timeLabel = timeLabel
        self.asset = asset
        self.side = side
        self.quantityMicros = quantityMicros
        self.priceMicros = priceMicros
        self.venue = venue
    }
}
