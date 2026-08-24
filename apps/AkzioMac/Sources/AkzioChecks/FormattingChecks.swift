import ObservatoryKit

// The load-bearing rule: a missing optional renders as `Unavailable`, never `0`.
func runFormattingChecks() {
    Check.suite("Ppm formatting") {
        Check.equal(PpmFormatter.percent(ppm: 14_600), "+1.46%", "signed percent")
        Check.equal(PpmFormatter.percent(ppm: -2_300), "-0.23%", "negative percent")
        Check.equal(PpmFormatter.percent(ppm: 0), "±0.00%", "zero keeps a sign slot")

        Check.equal(PpmFormatter.percent(ppm: nil), "Unavailable", "nil percent")
        Check.equal(PpmFormatter.share(ppm: nil), "Unavailable", "nil share")
        Check.equal(PpmFormatter.ratio(ppm: nil), "Unavailable", "nil ratio")
        Check.equal(PpmFormatter.currency(micros: nil), "Unavailable", "nil currency")
        Check.equal(PpmFormatter.count(nil), "Unavailable", "nil count")
        Check.equal(PpmFormatter.latency(millis: nil), "Unavailable", "nil latency")
        Check.expect(PpmFormatter.fraction(ppm: nil) == nil, "nil fraction must not become 0")

        Check.equal(PpmFormatter.percent(ppm: nil, missing: .waiting), "Waiting", "waiting label")
        Check.equal(
            PpmFormatter.currency(micros: nil, missing: .notApplicable),
            "Not Applicable",
            "not-applicable label"
        )
        Check.equal(PpmFormatter.elapsed(seconds: nil), "Pending", "pending elapsed")

        Check.equal(PpmFormatter.currency(micros: 1_028_645_720_000), "$1,028,645.72", "grouped currency")
        Check.equal(
            PpmFormatter.currency(micros: 14_960_210_000, signed: true),
            "+$14,960.21",
            "signed positive currency"
        )
        Check.equal(
            PpmFormatter.currency(micros: -24_381_000_000, signed: true),
            "-$24,381.00",
            "signed negative currency"
        )

        Check.expect(PpmFormatter.fraction(ppm: 680_000) == 0.68, "fraction conversion")
        Check.expect(PpmFormatter.fraction(ppm: 2_000_000) == 1, "fraction clamps high")
        Check.expect(PpmFormatter.fraction(ppm: -5) == 0, "fraction clamps low")

        Check.equal(PpmFormatter.elapsed(seconds: 8027), "02:13:47", "elapsed clock")
        Check.equal(PpmFormatter.elapsed(seconds: 7), "00:00:07", "elapsed pads")
        Check.equal(PpmFormatter.latency(millis: 832), "832 ms", "sub-second latency")
        Check.equal(PpmFormatter.latency(millis: 1_280), "1.28 s", "second-scale latency")
    }

    Check.suite("Status semantics") {
        Check.equal(
            AkzioStatus.notTriggered.detail,
            "No material conflict detected",
            "not-triggered explanation"
        )
        Check.equal(
            AkzioStatus.notApplicable.detail,
            "This run does not submit Paper orders",
            "not-applicable explanation"
        )
        Check.expect(AkzioStatus.notTriggered.style.tone == .muted, "not-triggered must not look successful")
        Check.expect(!AkzioStatus.notTriggered.isLive, "not-triggered must not pulse")
        Check.expect(AkzioStatus.observing.isLive, "observing should pulse")
        Check.expect(
            AkzioStatus.completedWithRejection.style.tone == .coral,
            "execution rejection needs the coral tone"
        )
        for status in AkzioStatus.allCases {
            Check.expect(!status.style.symbol.isEmpty, "\(status.rawValue) is missing a symbol")
            Check.expect(!status.style.label.isEmpty, "\(status.rawValue) is missing a label")
        }
    }
}
