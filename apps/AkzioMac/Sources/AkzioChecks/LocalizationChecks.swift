import Foundation
import ObservatoryKit

func runLocalizationChecks() {
    Check.suite("Localization") {
        Check.equal(
            L10n.text("Overview", language: .simplifiedChinese),
            "总览",
            "Chinese navigation"
        )
        Check.equal(
            L10n.text("Run", language: .simplifiedChinese),
            "运行",
            "Chinese real-model run action"
        )
        Check.equal(
            L10n.text("Application & Core", language: .simplifiedChinese),
            "应用与核心",
            "Chinese settings shell"
        )
        Check.equal(
            L10n.text("Core Readiness", language: .simplifiedChinese),
            "核心就绪度",
            "Chinese live health metric"
        )
        Check.equal(
            L10n.text("Rust Core", language: .simplifiedChinese),
            "Rust 核心",
            "Chinese Core settings"
        )
        Check.equal(
            L10n.text("Unavailable from observer", language: .simplifiedChinese),
            "观察器未提供此数据",
            "Chinese missing live data"
        )
        Check.equal(
            L10n.text("Overview", language: .english),
            "Overview",
            "English passthrough"
        )
        Check.expect(
            !SettingsPresentation.Category.allCases.contains { $0.rawValue == "mockData" },
            "mock data not exposed settings category"
        )
        for category in SettingsPresentation.Category.allCases {
            Check.expect(
                L10n.text(category.displayName, language: .simplifiedChinese) != category.displayName,
                "\(category.rawValue) settings category Chinese translation"
            )
        }
        for route in AppRoute.primary {
            Check.expect(
                L10n.text(route.title, language: .simplifiedChinese) != route.title,
                "\(route.rawValue) navigation title Chinese translation"
            )
            Check.expect(
                L10n.text(route.headline, language: .simplifiedChinese) != route.headline,
                "\(route.rawValue) page headline Chinese translation"
            )
        }

        MainActor.assumeIsolated {
            let suiteName = "AkzioChecks.AppLanguage.\(UUID().uuidString)"
            guard let defaults = UserDefaults(suiteName: suiteName) else {
                Check.expect(false, "isolated language defaults suite is available")
                return
            }
            defer { defaults.removePersistentDomain(forName: suiteName) }

            let firstLaunch = ObservatoryStore(autoStartsCore: true, languageDefaults: defaults)
            firstLaunch.settings.language = .simplifiedChinese

            let relaunched = ObservatoryStore(autoStartsCore: true, languageDefaults: defaults)
            Check.equal(
                relaunched.settings.language,
                .simplifiedChinese,
                "Live App restores selected language after relaunch"
            )

            let capture = ObservatoryStore(autoStartsCore: false, languageDefaults: defaults)
            Check.equal(
                capture.settings.language,
                .system,
                "Capture ignores persisted App language"
            )
            capture.settings.language = .english

            let liveAfterCapture = ObservatoryStore(autoStartsCore: true, languageDefaults: defaults)
            Check.equal(
                liveAfterCapture.settings.language,
                .simplifiedChinese,
                "Capture never overwrites persisted App language"
            )
        }
    }
}
