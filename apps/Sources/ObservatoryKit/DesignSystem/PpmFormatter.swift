import Foundation

// 文件职责：把 Rust 传来的 ppm/micros/整数和可选缺失值格式化为固定 UI 文本或进度 fraction。
// Formatter 只读入值并返回 String/Double?；nil 通过 MissingValue 表达，不在 UI 层偷偷变成 0。
// MARK: - Unit formatting
//
// The Rust domain stores money/quantities as micros and ratios as ppm integers.
// The UI formats from those integers; it never receives pre-rounded floats and
// never substitutes `0` for a missing optional.
public enum PpmFormatter {
    // Rust 的 ppm/micros 单位统一用一百万作为换算基数，避免各个 View 重复写比例常数。
    public static let ppmPerUnit: Double = 1_000_000

    // MARK: Percent

    /// `+1.46%` — ppm to percent with a fixed sign slot.
    public static func percent(
        ppm: Int?,
        fractionDigits: Int = 2,
        signed: Bool = true,
        missing: MissingValue = .unavailable
    ) -> String {
        // 输入可选 ppm；有值时转成百分比并委托 decimal，nil 直接返回语义缺失文案。
        guard let ppm else { return missing.rawValue }
        let value = Double(ppm) / ppmPerUnit * 100
        return decimal(value, fractionDigits: fractionDigits, signed: signed) + "%"
    }

    /// Plain magnitude percent used for progress / completeness (no sign slot).
    public static func share(ppm: Int?, fractionDigits: Int = 1, missing: MissingValue = .unavailable) -> String {
        // share 复用 percent 但关闭正负号槽，适合进度/完整度等只表达幅度的读数。
        percent(ppm: ppm, fractionDigits: fractionDigits, signed: false, missing: missing)
    }

    /// 0…1 progress fraction for rings and bars; `nil` means "draw nothing".
    public static func fraction(ppm: Int?) -> Double? {
        // 输入 ppm，输出 nil 或夹在 0...1 的 Double；夹紧保证 Ring/Bar 不越界，nil 仍表示未知而非零。
        guard let ppm else { return nil }
        return min(max(Double(ppm) / ppmPerUnit, 0), 1)
    }

    // MARK: Money

    /// `$1,028,645.72` from micros.
    public static func currency(
        micros: Int64?,
        signed: Bool = false,
        fractionDigits: Int = 2,
        missing: MissingValue = .unavailable
    ) -> String {
        // 输入 micros，先换算为货币单位，再按 signed 选择 signPrefix 或普通负号，最后用固定分组格式拼接美元符号。
        guard let micros else { return missing.rawValue }
        let value = Double(micros) / ppmPerUnit
        let sign = signed ? signPrefix(value) : (value < 0 ? "-" : "")
        let formatter = NumberFormatter.grouping(fractionDigits: fractionDigits)
        let body = formatter.string(from: NSNumber(value: abs(value))) ?? "0"
        return "\(sign)$\(body)"
    }

    /// Quantities are whole shares in this system, but still stored as micros.
    public static func quantity(micros: Int64?, missing: MissingValue = .unavailable) -> String {
        // 整数份额显示 0 位小数，非整数 micros 保留 2 位；nil 使用 MissingValue，不与真实数量混淆。
        guard let micros else { return missing.rawValue }
        let value = Double(micros) / ppmPerUnit
        let formatter = NumberFormatter.grouping(fractionDigits: value == value.rounded() ? 0 : 2)
        return formatter.string(from: NSNumber(value: value)) ?? "0"
    }

    public static func price(micros: Int64?, missing: MissingValue = .unavailable) -> String {
        // 价格固定两位小数并使用同一 POSIX 分组格式，调用方不需要重复 NumberFormatter 配置。
        guard let micros else { return missing.rawValue }
        let formatter = NumberFormatter.grouping(fractionDigits: 2)
        return formatter.string(from: NSNumber(value: Double(micros) / ppmPerUnit)) ?? "0.00"
    }

    // MARK: Scalars

    public static func ratio(ppm: Int?, fractionDigits: Int = 2, missing: MissingValue = .unavailable) -> String {
        // ratio 把 ppm 还原成无单位小数；不添加百分号，语义由调用方字段决定。
        guard let ppm else { return missing.rawValue }
        return decimal(Double(ppm) / ppmPerUnit, fractionDigits: fractionDigits, signed: false)
    }

    public static func multiple(ppm: Int?, missing: MissingValue = .unavailable) -> String {
        // multiple 保留两位并追加 x，用于 text scale 等倍数设置的可读回显。
        guard let ppm else { return missing.rawValue }
        return decimal(Double(ppm) / ppmPerUnit, fractionDigits: 2, signed: false) + "x"
    }

    public static func count(_ value: Int?, missing: MissingValue = .unavailable) -> String {
        // count 只负责整数分组；可选值缺失时沿用 MissingValue。
        guard let value else { return missing.rawValue }
        return NumberFormatter.grouping(fractionDigits: 0).string(from: NSNumber(value: value)) ?? "0"
    }

    public static func latency(millis: Int?, missing: MissingValue = .unavailable) -> String {
        // 小于 1000ms 保持毫秒，大值转秒；这是展示分支，不改变底层 duration。
        guard let millis else { return missing.rawValue }
        if millis < 1000 { return "\(millis) ms" }
        return decimal(Double(millis) / 1000, fractionDigits: 2, signed: false) + " s"
    }

    /// `02:13:47` elapsed clock — fixed width so the status bar never reflows.
    public static func elapsed(seconds: Int?, missing: MissingValue = .pending) -> String {
        // nil 或负秒数返回 pending/unavailable；合法秒数拆成时分秒并固定两位，保证状态栏宽度稳定。
        guard let seconds, seconds >= 0 else { return missing.rawValue }
        let h = seconds / 3600
        let m = (seconds % 3600) / 60
        let s = seconds % 60
        return String(format: "%02d:%02d:%02d", h, m, s)
    }

    public static func duration(seconds: Int?, missing: MissingValue = .unavailable) -> String {
        // duration 是 elapsed 的语义别名，保留相同的缺失和固定宽度行为。
        elapsed(seconds: seconds, missing: missing)
    }

    // MARK: Helpers

    public static func signPrefix(_ value: Double) -> String {
        // 输入 Double，输出正/负/零的固定符号；零用 ± 明确表示不是正负方向。
        if value > 0 { return "+" }
        if value < 0 { return "-" }
        return "±"
    }

    private static func decimal(_ value: Double, fractionDigits: Int, signed: Bool) -> String {
        // 统一绝对值分组与符号拼接；NumberFormatter 失败时只在已知有值的路径回退为 "0"。
        let formatter = NumberFormatter.grouping(fractionDigits: fractionDigits)
        let body = formatter.string(from: NSNumber(value: abs(value))) ?? "0"
        let sign = signed ? signPrefix(value) : (value < 0 ? "-" : "")
        return sign + body
    }
}

extension NumberFormatter {
    // 输入小数位，输出 en_US_POSIX、固定最小/最大小数位并启用逗号分组的 formatter 值。
    static func grouping(fractionDigits: Int) -> NumberFormatter {
        let formatter = NumberFormatter()
        formatter.numberStyle = .decimal
        formatter.usesGroupingSeparator = true
        formatter.groupingSeparator = ","
        formatter.decimalSeparator = "."
        formatter.minimumFractionDigits = fractionDigits
        formatter.maximumFractionDigits = fractionDigits
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }
}
