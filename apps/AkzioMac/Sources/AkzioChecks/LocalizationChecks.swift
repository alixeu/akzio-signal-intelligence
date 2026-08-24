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
            "mock data is not exposed as a settings category"
        )
        for category in SettingsPresentation.Category.allCases {
            Check.expect(
                L10n.text(category.displayName, language: .simplifiedChinese) != category.displayName,
                "\(category.rawValue) settings category has a Chinese translation"
            )
        }
        for route in AppRoute.primary {
            Check.expect(
                L10n.text(route.title, language: .simplifiedChinese) != route.title,
                "\(route.rawValue) navigation title has a Chinese translation"
            )
            Check.expect(
                L10n.text(route.headline, language: .simplifiedChinese) != route.headline,
                "\(route.rawValue) page headline has a Chinese translation"
            )
            Check.expect(
                L10n.text(route.subtitle, language: .simplifiedChinese) != route.subtitle,
                "\(route.rawValue) page subtitle has a Chinese translation"
            )
        }
        for edge in WorkflowEdgeKind.allCases {
            Check.expect(
                L10n.text(edge.displayName, language: .simplifiedChinese) != edge.displayName,
                "\(edge.rawValue) workflow relation renders in Chinese"
            )
        }
        for tab in LearningPresentation.Tab.allCases {
            Check.expect(
                L10n.text(tab.displayName, language: .simplifiedChinese) != tab.displayName,
                "\(tab.rawValue) learning tab renders in Chinese"
            )
        }
        for category in RetrospectiveCategory.allCases {
            Check.expect(
                L10n.text(category.displayName, language: .simplifiedChinese) != category.displayName,
                "\(category.rawValue) learning category renders in Chinese"
            )
        }
        for sortKey in RunSortKey.allCases {
            Check.expect(
                L10n.text(sortKey.title, language: .simplifiedChinese) != sortKey.title,
                "\(sortKey.rawValue) Archive table header renders in Chinese"
            )
        }
        Check.equal(
            "2/5 \(L10n.text("Trading Sessions", language: .simplifiedChinese))",
            "2/5 交易会话",
            "Outcome and Workflow trading-session progress is localized as rendered"
        )
        Check.equal(
            "3 \(L10n.text("shown", language: .simplifiedChinese)) · 8 \(L10n.text("matching", language: .simplifiedChinese))",
            "3 已显示 · 8 条匹配",
            "Archive result summary is localized as rendered"
        )
        Check.equal(
            L10n.text("2M AGO", language: .simplifiedChinese),
            "2 分钟前",
            "Overview relative timestamp is localized dynamically"
        )
        Check.equal(
            L10n.text("Aug 17", language: .simplifiedChinese),
            "8月17日",
            "Archive month/day label is localized dynamically"
        )
        Check.equal(
            L10n.text("1D · vs QQQ", language: .simplifiedChinese),
            "1D · 对比 QQQ",
            "Portfolio comparison keeps technical range and ticker while localizing prose"
        )
        Check.equal(
            L10n.text("Paper Running — Synthesizer Active", language: .simplifiedChinese),
            "Paper 运行中 — 综合者活跃",
            "Overview scenario status is localized"
        )
        Check.equal(
            L10n.text(OutcomeHorizonKind.t3.windowLabel, language: .simplifiedChinese),
            "3 个交易会话",
            "Outcome ring window is localized from its presentation value"
        )
        Check.equal(
            L10n.text("Claims are consistent with evidence set; proceeding along critical path.", language: .simplifiedChinese),
            "论点与证据集一致，沿关键路径继续。",
            "Workflow observed summary is localized"
        )
        Check.equal(
            L10n.text(
                "No tool calls — the LLM returned its result without invoking a tool.",
                language: .simplifiedChinese
            ),
            "未调用工具——LLM 未调用工具即返回结果。",
            "zero-tool transcript localized"
        )
        Check.equal(
            L10n.text("gpt-5.6-luna", language: .simplifiedChinese),
            "gpt-5.6-luna",
            "technical model identifier remains unchanged"
        )
        Check.equal(
            L10n.text(AkzioStatus.completedWithRejection.style.label, language: .simplifiedChinese),
            "已完成 · 执行已拒绝",
            "Archive composite status value is localized"
        )

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
