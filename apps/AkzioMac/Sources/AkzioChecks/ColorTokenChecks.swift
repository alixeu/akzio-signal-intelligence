import ObservatoryKit

// The palette guard rail: the spec bans purple / violet / blue-violet outright,
// and restricts green to tiny success dots.
func runColorTokenChecks() {
    Check.suite("Color tokens") {
        let palette: [(String, UInt32)] = [
            ("appBackground", 0x1A1A1A),
            ("deepBackground", 0x1D1D1D),
            ("raisedSurface", 0x232323),
            ("elevatedSurface", 0x292929),
            ("primaryGold", 0xD4A15E),
            ("actionCoral", 0xFF6B4A),
            ("primaryText", 0xF3EFE9),
            ("secondaryText", 0xB0A9A0),
            ("mutedText", 0x948D84),
            ("successDot", 0x5E8F6B),
        ]
        for (name, hex) in palette {
            Check.expect(
                !ColorProbe(hex: hex).isBannedHue,
                "\(name) falls inside the banned blue/purple band"
            )
        }

        let gold = ColorProbe(hex: 0xD4A15E)
        Check.expect(gold.hue > 20 && gold.hue < 50, "primaryGold is not a warm hue")

        let coral = ColorProbe(hex: 0xFF6B4A)
        Check.expect(coral.hue >= 0 && coral.hue < 25, "actionCoral is not a warm hue")

        let green = ColorProbe(hex: 0x5E8F6B)
        Check.expect(green.saturation < 0.40, "successDot is too saturated to stay a micro accent")
        Check.expect(green.brightness < 0.60, "successDot is too bright to stay a micro accent")

        // Theme variants must stay inside the deep-grey band: dark surfaces only,
        // and never drift into the banned hues.
        let themeHexes: [(String, UInt32)] = [
            ("dusk background", 0x211F1D),
            ("midnight background", 0x181818),
            ("dusk surface", 0x272421),
            ("midnight surface", 0x202020),
        ]
        for (name, hex) in themeHexes {
            let probe = ColorProbe(hex: hex)
            Check.expect(!probe.isBannedHue, "\(name) falls inside the banned blue/purple band")
            Check.expect(probe.brightness < 0.20, "\(name) is too bright for a deep-grey theme")
            Check.expect(probe.saturation < 0.20, "\(name) is too saturated for a deep-grey theme")
        }

        let tones: [AkzioTone] = [.gold, .coral, .neutral, .muted]
        Check.equal(Set(tones).count, tones.count, "tones must be distinct")
    }
}
