import Foundation

// MARK: - Settings
//
// Presentation preferences are session-local. Core configuration is persisted
// in ~/.akzio/config.toml and exposed through ObservatoryStore.
public struct SettingsPresentation: Sendable, Hashable {
    public enum Category: String, CaseIterable, Sendable, Identifiable {
        case appearance
        case motion
        case modelDisplay
        case core
        case accessibility
        case environment

        public var id: String { rawValue }

        public var displayName: String {
            switch self {
            case .appearance: "Appearance"
            case .motion: "Motion"
            case .modelDisplay: "Model Display"
            case .core: "Core"
            case .accessibility: "Accessibility"
            case .environment: "Environment Info"
            }
        }

        public var symbol: String {
            switch self {
            case .appearance: "paintpalette"
            case .motion: "wand.and.rays"
            case .modelDisplay: "cube.transparent"
            case .core: "server.rack"
            case .accessibility: "accessibility"
            case .environment: "info.circle"
            }
        }
    }

    public enum Theme: String, CaseIterable, Sendable, Identifiable {
        case dark, dusk, midnight

        public var id: String { rawValue }
        public var displayName: String { rawValue.capitalized }
    }

    public enum Density: String, CaseIterable, Sendable, Identifiable {
        case compact, comfortable, spacious

        public var id: String { rawValue }
        public var displayName: String { rawValue.capitalized }

        /// Multiplier applied to card padding and row height.
        public var scale: Double {
            switch self {
            case .compact: 0.88
            case .comfortable: 1.0
            case .spacious: 1.12
            }
        }
    }

    public enum LabelDensity: String, CaseIterable, Sendable, Identifiable {
        case auto, minimal, full

        public var id: String { rawValue }
        public var displayName: String { rawValue.capitalized }
    }

    // Appearance
    public var language: AppLanguage
    public var theme: Theme
    public var density: Density
    public var glassIntensity: GlassIntensity
    public var glassTransparency: Double

    // Motion
    public var globalMotionEnabled: Bool
    public var motionIntensity: Double
    public var particleDensity: Double
    public var routeTransitionStrength: Double

    // Model display
    public var renderQuality: CanvasRenderPolicy.Quality
    public var labelDensity: LabelDensity
    public var showsReasoningVisualization: Bool

    // Accessibility (user overrides layered on top of the system settings)
    public var reduceMotionOverride: Bool
    public var reduceTransparencyOverride: Bool
    public var highContrast: Bool
    public var textScale: Double
    public var colorIndependentStatus: Bool
    public var keyboardFocusVisible: Bool

    public init(
        language: AppLanguage = .system,
        theme: Theme = .dark,
        density: Density = .comfortable,
        glassIntensity: GlassIntensity = .medium,
        glassTransparency: Double = 0.4,
        globalMotionEnabled: Bool = true,
        motionIntensity: Double = 1.0,
        particleDensity: Double = 1.0,
        routeTransitionStrength: Double = 1.0,
        renderQuality: CanvasRenderPolicy.Quality = .high,
        labelDensity: LabelDensity = .auto,
        showsReasoningVisualization: Bool = true,
        reduceMotionOverride: Bool = false,
        reduceTransparencyOverride: Bool = false,
        highContrast: Bool = false,
        textScale: Double = 1.0,
        colorIndependentStatus: Bool = true,
        keyboardFocusVisible: Bool = true
    ) {
        self.language = language
        self.theme = theme
        self.density = density
        self.glassIntensity = glassIntensity
        self.glassTransparency = glassTransparency
        self.globalMotionEnabled = globalMotionEnabled
        self.motionIntensity = motionIntensity
        self.particleDensity = particleDensity
        self.routeTransitionStrength = routeTransitionStrength
        self.renderQuality = renderQuality
        self.labelDensity = labelDensity
        self.showsReasoningVisualization = showsReasoningVisualization
        self.reduceMotionOverride = reduceMotionOverride
        self.reduceTransparencyOverride = reduceTransparencyOverride
        self.highContrast = highContrast
        self.textScale = textScale
        self.colorIndependentStatus = colorIndependentStatus
        self.keyboardFocusVisible = keyboardFocusVisible
    }

    /// Read-only environment facts shown in the last category.
    public static let environmentRows: [(String, String, String)] = [
        ("macOS", "27.0 (26A5416b)", "apple.logo"),
        ("Toolchain", "Swift 6.2.3 · CLT", "hammer"),
        ("SDK", "macOS 26.2", "shippingbox"),
        ("SwiftUI", "Observation + Charts", "square.stack.3d.up"),
        ("Device", "Apple Silicon (arm64)", "cpu"),
        ("Renderer", "Canvas + TimelineView", "sparkles"),
    ]
}
