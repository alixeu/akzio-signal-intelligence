import Foundation

// MARK: - Unit formatting
//
// The Rust domain stores money/quantities as micros and ratios as ppm integers.
// The UI formats from those integers; it never receives pre-rounded floats and
// never substitutes `0` for a missing optional.
public enum PpmFormatter {
    public static let ppmPerUnit: Double = 1_000_000

    // MARK: Percent

    /// `+1.46%` — ppm to percent with a fixed sign slot.
    public static func percent(
        ppm: Int?,
        fractionDigits: Int = 2,
        signed: Bool = true,
        missing: MissingValue = .unavailable
    ) -> String {
        guard let ppm else { return missing.rawValue }
        let value = Double(ppm) / ppmPerUnit * 100
        return decimal(value, fractionDigits: fractionDigits, signed: signed) + "%"
    }

    /// Plain magnitude percent used for progress / completeness (no sign slot).
    public static func share(ppm: Int?, fractionDigits: Int = 1, missing: MissingValue = .unavailable) -> String {
        percent(ppm: ppm, fractionDigits: fractionDigits, signed: false, missing: missing)
    }

    /// 0…1 progress fraction for rings and bars; `nil` means "draw nothing".
    public static func fraction(ppm: Int?) -> Double? {
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
        guard let micros else { return missing.rawValue }
        let value = Double(micros) / ppmPerUnit
        let sign = signed ? signPrefix(value) : (value < 0 ? "-" : "")
        let formatter = NumberFormatter.grouping(fractionDigits: fractionDigits)
        let body = formatter.string(from: NSNumber(value: abs(value))) ?? "0"
        return "\(sign)$\(body)"
    }

    /// Quantities are whole shares in this system, but still stored as micros.
    public static func quantity(micros: Int64?, missing: MissingValue = .unavailable) -> String {
        guard let micros else { return missing.rawValue }
        let value = Double(micros) / ppmPerUnit
        let formatter = NumberFormatter.grouping(fractionDigits: value == value.rounded() ? 0 : 2)
        return formatter.string(from: NSNumber(value: value)) ?? "0"
    }

    public static func price(micros: Int64?, missing: MissingValue = .unavailable) -> String {
        guard let micros else { return missing.rawValue }
        let formatter = NumberFormatter.grouping(fractionDigits: 2)
        return formatter.string(from: NSNumber(value: Double(micros) / ppmPerUnit)) ?? "0.00"
    }

    // MARK: Scalars

    public static func ratio(ppm: Int?, fractionDigits: Int = 2, missing: MissingValue = .unavailable) -> String {
        guard let ppm else { return missing.rawValue }
        return decimal(Double(ppm) / ppmPerUnit, fractionDigits: fractionDigits, signed: false)
    }

    public static func multiple(ppm: Int?, missing: MissingValue = .unavailable) -> String {
        guard let ppm else { return missing.rawValue }
        return decimal(Double(ppm) / ppmPerUnit, fractionDigits: 2, signed: false) + "x"
    }

    public static func count(_ value: Int?, missing: MissingValue = .unavailable) -> String {
        guard let value else { return missing.rawValue }
        return NumberFormatter.grouping(fractionDigits: 0).string(from: NSNumber(value: value)) ?? "0"
    }

    public static func latency(millis: Int?, missing: MissingValue = .unavailable) -> String {
        guard let millis else { return missing.rawValue }
        if millis < 1000 { return "\(millis) ms" }
        return decimal(Double(millis) / 1000, fractionDigits: 2, signed: false) + " s"
    }

    /// `02:13:47` elapsed clock — fixed width so the status bar never reflows.
    public static func elapsed(seconds: Int?, missing: MissingValue = .pending) -> String {
        guard let seconds, seconds >= 0 else { return missing.rawValue }
        let h = seconds / 3600
        let m = (seconds % 3600) / 60
        let s = seconds % 60
        return String(format: "%02d:%02d:%02d", h, m, s)
    }

    public static func duration(seconds: Int?, missing: MissingValue = .unavailable) -> String {
        elapsed(seconds: seconds, missing: missing)
    }

    // MARK: Helpers

    public static func signPrefix(_ value: Double) -> String {
        if value > 0 { return "+" }
        if value < 0 { return "-" }
        return "±"
    }

    private static func decimal(_ value: Double, fractionDigits: Int, signed: Bool) -> String {
        let formatter = NumberFormatter.grouping(fractionDigits: fractionDigits)
        let body = formatter.string(from: NSNumber(value: abs(value))) ?? "0"
        let sign = signed ? signPrefix(value) : (value < 0 ? "-" : "")
        return sign + body
    }
}

extension NumberFormatter {
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
