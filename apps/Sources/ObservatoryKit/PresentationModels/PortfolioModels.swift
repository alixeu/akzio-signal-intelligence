import Foundation

// MARK: - Portfolio

public struct EquityPoint: Sendable, Hashable, Identifiable {
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
        self.index = index
        self.minutesFromOpen = minutesFromOpen
        self.timestamp = timestamp
        self.portfolio = portfolio
        self.benchmark = benchmark
    }

    /// Clock label derived from the frozen anchor, never from `Date()`.
    public var timeLabel: String {
        let totalMinutes = 9 * 60 + 30 + minutesFromOpen
        return String(format: "%02d:%02d", totalMinutes / 60, totalMinutes % 60)
    }

    public var chartX: Double {
        timestamp?.timeIntervalSinceReferenceDate ?? Double(index)
    }

    public func axisLabel(for range: EquityRange, locale: Locale) -> String {
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
    public let label: String
    public let actualPpm: Int
    public let targetPpm: Int

    public var id: String { label }

    public init(label: String, actualPpm: Int, targetPpm: Int) {
        self.label = label
        self.actualPpm = actualPpm
        self.targetPpm = targetPpm
    }

    public var deltaPpm: Int { actualPpm - targetPpm }
    public var isOverweight: Bool { deltaPpm > 0 }
}

public struct PositionPresentation: Sendable, Hashable, Identifiable {
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
