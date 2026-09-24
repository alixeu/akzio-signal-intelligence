import Foundation

public enum CoreModelStage: String, CaseIterable, Codable, Sendable, Hashable, Identifiable {
    // Raw value 是 Rust 配置中的稳定 route key；Hashable/Identifiable 让字典和 SwiftUI 列表都使用同一身份。
    case analyst = "research.analyst"
    case critic = "research.critic"
    case synthesizer = "research.synthesizer"
    case outcomeReviewer = "learning.outcome_worker"

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .analyst: "Analyst"
        case .critic: "Critic"
        case .synthesizer: "Synthesizer"
        case .outcomeReviewer: "Outcome Reviewer"
        }
    }
}

public struct CoreStageModelConfiguration: Codable, Sendable, Equatable {
    // 配置模型是值语义快照；Codable 的 CodingKeys 只负责 Swift 属性与既有 JSON/TOML 投影字段对齐。
    public var model: String
    public var reasoningEffort: String
    public var responseLanguage: String?

    public init(model: String, reasoningEffort: String, responseLanguage: String? = nil) {
        self.model = model
        self.reasoningEffort = reasoningEffort
        self.responseLanguage = responseLanguage
    }

    enum CodingKeys: String, CodingKey {
        case model
        case reasoningEffort = "reasoning_effort"
        case responseLanguage = "response_language"
    }
}

public struct CoreConfiguration: Codable, Sendable, Equatable {
    // 这是已经解析的配置值，不暴露 Codable 自定义逻辑；密钥只在受控启动/保存路径中流转。
    public let provider: String
    public let llmBaseURL: String
    public let llmAPIKey: String
    public let globalModel: String
    public let globalReasoningEffort: String
    public let globalResponseLanguage: String
    public let stageModels: [CoreModelStage: CoreStageModelConfiguration]
    public let alpacaAPIKey: String
    public let alpacaAPISecret: String
    public let fredAPIKey: String?
    public let secUserAgent: String?

    public var isComplete: Bool {
        // isComplete 只判断必填值是否存在；它不验证 provider 能力、模型可用性或 Paper 业务资格。
        !provider.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !llmBaseURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !llmAPIKey.isEmpty
            && !globalModel.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !globalReasoningEffort.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !globalResponseLanguage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        && stageModels.values.allSatisfy {
            !$0.model.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            && !$0.reasoningEffort.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
        && !alpacaAPIKey.isEmpty
        && !alpacaAPISecret.isEmpty
        && fredAPIKey?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
    }
}

public struct CoreCredentialStatus: Sendable, Equatable {
    // Status 只携带布尔存在性，不把 API key/secret 回显给 Settings 或日志。
    public let llmAPIKey: Bool
    public let alpacaAPIKey: Bool
    public let alpacaAPISecret: Bool
    public let fredAPIKey: Bool

    public init(
        llmAPIKey: Bool,
        alpacaAPIKey: Bool,
        alpacaAPISecret: Bool,
        fredAPIKey: Bool
    ) {
        self.llmAPIKey = llmAPIKey
        self.alpacaAPIKey = alpacaAPIKey
        self.alpacaAPISecret = alpacaAPISecret
        self.fredAPIKey = fredAPIKey
    }

}

public struct CoreConfigurationDraft: Sendable, Equatable {
    // Draft 是 Settings 表单的可变值副本；保存前仍需经过 trim、完整性校验和 Core CLI 写入。
    public var provider = "openai_responses"
    public var llmBaseURL = ""
    public var llmAPIKey = ""
    public var globalModel = "gpt-5.6-luna"
    public var globalReasoningEffort = "low"
    public var globalResponseLanguage = "简体中文"
    public var stageModels = Dictionary(
        uniqueKeysWithValues: CoreModelStage.allCases.map {
            (
                $0,
                CoreStageModelConfiguration(
                    model: "gpt-5.6-luna",
                    reasoningEffort: "low",
                    responseLanguage: nil
                )
            )
        }
    )
    public var alpacaAPIKey = ""
    public var alpacaAPISecret = ""
    public var fredAPIKey = ""
    public var secUserAgent = ""

    public init() {}
}

enum CoreCredentialError: LocalizedError {
    case configuration(String)
    case incompleteRequiredConfiguration

    var errorDescription: String? {
        switch self {
        case .configuration(let message):
            "Core configuration operation failed: \(message)"
        case .incompleteRequiredConfiguration:
            "Provider, LLM URL, LLM API key, Alpaca API key, Alpaca API secret, and FRED API key are required"
        }
    }
}

enum CoreRuntimePaths {
    static func executableURL() throws -> URL {
        // 可执行文件优先尊重显式环境覆盖，再按 bundle/debug/release 候选查找；找不到时返回启动错误。
        let environment = ProcessInfo.processInfo.environment
        if let path = environment["AKZIO_CORE_EXECUTABLE"], !path.isEmpty {
            return URL(fileURLWithPath: path)
        }
        let fileManager = FileManager.default
        let bundleCandidate = Bundle.main.bundleURL.appending(path: "Contents/MacOS/akzio-core")
        let cwd = URL(fileURLWithPath: fileManager.currentDirectoryPath)
        let candidates = [
            bundleCandidate,
            cwd.appending(path: "target/debug/akzio"),
            cwd.appending(path: "target/release/akzio"),
            cwd.appending(path: "../../target/debug/akzio").standardizedFileURL,
            cwd.appending(path: "../../target/release/akzio").standardizedFileURL,
        ]
        guard let candidate = candidates.first(where: {
            fileManager.isExecutableFile(atPath: $0.path)
        }) else {
            throw CoreLaunchError.missingExecutable
        }
        return candidate
    }

    static func homeURL(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        homeDirectory: URL = FileManager.default.homeDirectoryForCurrentUser
    ) throws -> URL {
        // home 是本地运行时根目录；创建后立即收紧到 0700，避免 Store、日志和 token 被其他用户读取。
        let home: URL
        if let path = environment["AKZIO_HOME"], !path.isEmpty {
            home = URL(fileURLWithPath: path, isDirectory: true)
        } else {
            home = homeDirectory.appending(path: ".akzio", directoryHint: .isDirectory)
        }
        try FileManager.default.createDirectory(at: home, withIntermediateDirectories: true)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o700],
            ofItemAtPath: home.path
        )
        return home
    }

    static func configurationLocation(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        homeDirectory: URL = FileManager.default.homeDirectoryForCurrentUser
    ) -> URL {
        // 路径选择是纯环境/默认值解析；真正创建配置和权限设置由 configURL 负责。
        if let path = environment["AKZIO_CORE_CONFIG"], !path.isEmpty {
            return URL(fileURLWithPath: path)
        }
        let home = environment["AKZIO_HOME"].flatMap { $0.isEmpty ? nil : URL(fileURLWithPath: $0, isDirectory: true) }
            ?? homeDirectory.appending(path: ".akzio", directoryHint: .isDirectory)
        return home.appending(path: "config.toml")
    }

    static func configURL() throws -> URL {
        // configURL 首次使用时通过 Rust observatory-config 初始化，而不是 Swift 复制模板语义。
        let environment = ProcessInfo.processInfo.environment
        if let path = environment["AKZIO_CORE_CONFIG"], !path.isEmpty {
            return URL(fileURLWithPath: path)
        }
        _ = try homeURL()
        let config = configurationLocation()
        if !FileManager.default.fileExists(atPath: config.path) {
            try initializeConfiguration(at: config)
        }
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600],
            ofItemAtPath: config.path
        )
        return config
    }

    static func storeURL() throws -> URL {
        // Store 路径始终位于同一受保护 AKZIO home 下，Debug/隔离边界由 Core 配置和 Store 自己执行。
        try homeURL().appending(path: "store", directoryHint: .isDirectory)
    }

    static func logURL() throws -> URL {
        let logs = try homeURL().appending(path: "logs", directoryHint: .isDirectory)
        try FileManager.default.createDirectory(at: logs, withIntermediateDirectories: true)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o700],
            ofItemAtPath: logs.path
        )
        return logs.appending(path: "core.log")
    }

    private static func bundledConfigURL() throws -> URL {
        let fileManager = FileManager.default
        let cwd = URL(fileURLWithPath: fileManager.currentDirectoryPath)
        let candidates = [
            Bundle.main.url(forResource: "akzio.observatory", withExtension: "toml"),
            cwd.appending(path: "config/akzio.observatory.toml"),
            cwd.appending(path: "../../config/akzio.observatory.toml").standardizedFileURL,
        ].compactMap { $0 }
        guard let candidate = candidates.first(where: {
            fileManager.fileExists(atPath: $0.path)
        }) else {
            throw CoreLaunchError.missingConfig
        }
        return candidate
    }

    private static func initializeConfiguration(at config: URL) throws {
        // 初始化是一次受控子进程调用；只有退出码为 0 才把 stdout 之外的配置视为成功，stderr 仅转成错误信息。
        let process = Process()
        process.executableURL = try executableURL()
        process.arguments = [
            "observatory-config",
            "--config", config.path,
            "init",
            "--template", try bundledConfigURL().path,
            "--store-root", try storeURL().path,
        ]
        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            let message = String(
                data: stderr.fileHandleForReading.readDataToEndOfFile(),
                encoding: .utf8
            )?.trimmingCharacters(in: .whitespacesAndNewlines)
            throw CoreCredentialError.configuration(
                message.flatMap { $0.isEmpty ? nil : $0 }
                    ?? "core exited with status \(process.terminationStatus)"
            )
        }
    }
}

private struct CoreFileConfiguration: Codable {
    let provider: String
    let llmBaseURL: String
    let llmAPIKey: String
    let globalModel: String
    let globalReasoningEffort: String
    let globalResponseLanguage: String
    let stageModels: [CoreModelStage: CoreStageModelConfiguration]
    let alpacaAPIKey: String
    let alpacaAPISecret: String
    let fredAPIKey: String?
    let secUserAgent: String?

    enum CodingKeys: String, CodingKey {
        case provider
        case llmBaseURL
        case llmAPIKey
        case globalModel
        case globalReasoningEffort
        case globalResponseLanguage
        case stageModels
        case alpacaAPIKey
        case alpacaAPISecret
        case fredAPIKey
        case secUserAgent
    }

    init(
        provider: String,
        llmBaseURL: String,
        llmAPIKey: String,
        globalModel: String,
        globalReasoningEffort: String,
        globalResponseLanguage: String,
        stageModels: [CoreModelStage: CoreStageModelConfiguration],
        alpacaAPIKey: String,
        alpacaAPISecret: String,
        fredAPIKey: String?,
        secUserAgent: String?
    ) {
        self.provider = provider
        self.llmBaseURL = llmBaseURL
        self.llmAPIKey = llmAPIKey
        self.globalModel = globalModel
        self.globalReasoningEffort = globalReasoningEffort
        self.globalResponseLanguage = globalResponseLanguage
        self.stageModels = stageModels
        self.alpacaAPIKey = alpacaAPIKey
        self.alpacaAPISecret = alpacaAPISecret
        self.fredAPIKey = fredAPIKey
        self.secUserAgent = secUserAgent
    }

    init(from decoder: Decoder) throws {
        // 文件里的 stageModels 是 String key 字典；这里逐项映射到受限 CoreModelStage，未知 route 直接解码失败。
        let container = try decoder.container(keyedBy: CodingKeys.self)
        provider = try container.decode(String.self, forKey: .provider)
        llmBaseURL = try container.decode(String.self, forKey: .llmBaseURL)
        llmAPIKey = try container.decode(String.self, forKey: .llmAPIKey)
        globalModel = try container.decode(String.self, forKey: .globalModel)
        globalReasoningEffort = try container.decode(String.self, forKey: .globalReasoningEffort)
        globalResponseLanguage = try container.decode(String.self, forKey: .globalResponseLanguage)

        let rawStageModels = try container.decode(
            [String: CoreStageModelConfiguration].self,
            forKey: .stageModels
        )
        var decodedStageModels: [CoreModelStage: CoreStageModelConfiguration] = [:]
        for (rawStage, configuration) in rawStageModels {
            guard let stage = CoreModelStage(rawValue: rawStage) else {
                throw DecodingError.dataCorruptedError(
                    forKey: .stageModels,
                    in: container,
                    debugDescription: "Unsupported model route \(rawStage)"
                )
            }
            decodedStageModels[stage] = configuration
        }
        stageModels = decodedStageModels

        alpacaAPIKey = try container.decode(String.self, forKey: .alpacaAPIKey)
        alpacaAPISecret = try container.decode(String.self, forKey: .alpacaAPISecret)
        fredAPIKey = try container.decodeIfPresent(String.self, forKey: .fredAPIKey)
        secUserAgent = try container.decodeIfPresent(String.self, forKey: .secUserAgent)
    }

    func encode(to encoder: Encoder) throws {
        // 编码时把 enum key 还原为稳定 rawValue，保证写回格式与 Rust observatory-config 的字段契约一致。
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(provider, forKey: .provider)
        try container.encode(llmBaseURL, forKey: .llmBaseURL)
        try container.encode(llmAPIKey, forKey: .llmAPIKey)
        try container.encode(globalModel, forKey: .globalModel)
        try container.encode(globalReasoningEffort, forKey: .globalReasoningEffort)
        try container.encode(globalResponseLanguage, forKey: .globalResponseLanguage)
        let rawStageModels = Dictionary(uniqueKeysWithValues: stageModels.map {
            ($0.key.rawValue, $0.value)
        })
        try container.encode(rawStageModels, forKey: .stageModels)
        try container.encode(alpacaAPIKey, forKey: .alpacaAPIKey)
        try container.encode(alpacaAPISecret, forKey: .alpacaAPISecret)
        try container.encodeIfPresent(fredAPIKey, forKey: .fredAPIKey)
        try container.encodeIfPresent(secUserAgent, forKey: .secUserAgent)
    }
}

struct CoreCredentialStore {
    // CredentialStore 只通过 Rust CLI 的 get/set 读写配置；环境变量是草稿回退，不是另一份持久化权威。
    private enum ConfigurationAction: String {
        case get
        case set
    }

    static func status() -> CoreCredentialStatus {
        // status 允许配置不完整时仍渲染 Settings；try? 失败会落到环境草稿并只暴露存在性。
        let draft = savedDraft()
        return CoreCredentialStatus(
            llmAPIKey: hasValue(draft.llmAPIKey),
            alpacaAPIKey: hasValue(draft.alpacaAPIKey),
            alpacaAPISecret: hasValue(draft.alpacaAPISecret),
            fredAPIKey: hasValue(draft.fredAPIKey)
        )
    }

    static func savedDraft() -> CoreConfigurationDraft {
        // 文件读取失败不把 App 变成 mock；这里只返回表单可显示的环境/默认草稿，启动时仍会再次严格解析。
        (try? fileDraft()) ?? environmentDraft()
    }

    static func resolved() throws -> CoreConfiguration? {
        // resolved 的 nil 表示“配置文件可读但必填项未齐”，由 supervisor 显示 needsConfiguration。
        let draft = try fileDraft()
        let configuration = CoreConfiguration(
            provider: draft.provider,
            llmBaseURL: draft.llmBaseURL,
            llmAPIKey: draft.llmAPIKey,
            globalModel: draft.globalModel,
            globalReasoningEffort: draft.globalReasoningEffort,
            globalResponseLanguage: draft.globalResponseLanguage,
            stageModels: draft.stageModels,
            alpacaAPIKey: draft.alpacaAPIKey,
            alpacaAPISecret: draft.alpacaAPISecret,
            fredAPIKey: draft.fredAPIKey.isEmpty ? nil : draft.fredAPIKey,
            secUserAgent: draft.secUserAgent
        )
        return configuration.isComplete ? configuration : nil
    }

    static func save(_ draft: CoreConfigurationDraft) throws {
        // save 先构造清洗后的值类型配置，再交给 Core CLI；Swift 不直接编辑 TOML 或 SQLite。
        let configuration = CoreConfiguration(
            provider: draft.provider.trimmingCharacters(in: .whitespacesAndNewlines),
            llmBaseURL: draft.llmBaseURL.trimmingCharacters(in: .whitespacesAndNewlines),
            llmAPIKey: draft.llmAPIKey,
            globalModel: draft.globalModel.trimmingCharacters(in: .whitespacesAndNewlines),
            globalReasoningEffort: draft.globalReasoningEffort.trimmingCharacters(in: .whitespacesAndNewlines),
            globalResponseLanguage: draft.globalResponseLanguage.trimmingCharacters(in: .whitespacesAndNewlines),
            stageModels: draft.stageModels,
            alpacaAPIKey: draft.alpacaAPIKey,
            alpacaAPISecret: draft.alpacaAPISecret,
            fredAPIKey: draft.fredAPIKey.isEmpty ? nil : draft.fredAPIKey,
            secUserAgent: draft.secUserAgent.trimmingCharacters(in: .whitespacesAndNewlines)
        )
        guard configuration.isComplete else {
            throw CoreCredentialError.incompleteRequiredConfiguration
        }

        let fileConfiguration = CoreFileConfiguration(
            provider: configuration.provider,
            llmBaseURL: configuration.llmBaseURL,
            llmAPIKey: configuration.llmAPIKey,
            globalModel: configuration.globalModel,
            globalReasoningEffort: configuration.globalReasoningEffort,
            globalResponseLanguage: configuration.globalResponseLanguage,
            stageModels: configuration.stageModels,
            alpacaAPIKey: configuration.alpacaAPIKey,
            alpacaAPISecret: configuration.alpacaAPISecret,
            fredAPIKey: configuration.fredAPIKey,
            secUserAgent: configuration.secUserAgent
        )
        _ = try runConfigurationCommand(
            .set,
            input: JSONEncoder().encode(fileConfiguration)
        )
    }

    static func clear() throws {
        // clear 只清空凭据字段，保留 provider/模型/route 等非秘密设置；之后 Core 必须重新配置才能启动。
        var draft = try fileDraft()
        draft.llmAPIKey = ""
        draft.alpacaAPIKey = ""
        draft.alpacaAPISecret = ""
        draft.fredAPIKey = ""
        let configuration = CoreFileConfiguration(
            provider: draft.provider,
            llmBaseURL: draft.llmBaseURL,
            llmAPIKey: "",
            globalModel: draft.globalModel,
            globalReasoningEffort: draft.globalReasoningEffort,
            globalResponseLanguage: draft.globalResponseLanguage,
            stageModels: draft.stageModels,
            alpacaAPIKey: "",
            alpacaAPISecret: "",
            fredAPIKey: nil,
            secUserAgent: draft.secUserAgent
        )
        _ = try runConfigurationCommand(.set, input: JSONEncoder().encode(configuration))
    }

    private static func fileDraft() throws -> CoreConfigurationDraft {
        // fileDraft 读取 CLI 返回的 JSON，再把文件值与环境占位符解析到一个新的 Draft，不共享可变引用。
        let data = try runConfigurationCommand(.get)
        let configuration = try JSONDecoder().decode(CoreFileConfiguration.self, from: data)
        var draft = environmentDraft()
        draft.provider = configuration.provider
        draft.llmBaseURL = resolvedSavedValue(configuration.llmBaseURL, fallback: draft.llmBaseURL)
        draft.llmAPIKey = resolvedSavedValue(configuration.llmAPIKey, fallback: draft.llmAPIKey)
        draft.globalModel = configuration.globalModel
        draft.globalReasoningEffort = configuration.globalReasoningEffort
        draft.globalResponseLanguage = configuration.globalResponseLanguage
        draft.stageModels.merge(configuration.stageModels) { _, saved in saved }
        draft.alpacaAPIKey = resolvedSavedValue(configuration.alpacaAPIKey, fallback: draft.alpacaAPIKey)
        draft.alpacaAPISecret = resolvedSavedValue(configuration.alpacaAPISecret, fallback: draft.alpacaAPISecret)
        draft.fredAPIKey = resolvedSavedValue(configuration.fredAPIKey, fallback: draft.fredAPIKey)
        draft.secUserAgent = configuration.secUserAgent ?? ""
        return draft
    }

    private static func resolvedSavedValue(_ value: String, fallback: String) -> String {
        // $NAME 或 $NAME/suffix 是存储层的环境占位符；缺失环境变量时保留 fallback，不把未知占位符当秘密正文。
        guard value.hasPrefix("$") else { return value }
        let placeholder = String(value.dropFirst())
        let parts = placeholder.split(separator: "/", maxSplits: 1, omittingEmptySubsequences: false)
        guard let name = parts.first, !name.isEmpty,
              let environmentValue = ProcessInfo.processInfo.environment[String(name)]
        else {
            return fallback
        }
        guard parts.count == 2 else { return environmentValue }
        return environmentValue + "/" + parts[1]
    }

    private static func resolvedSavedValue(_ value: String?, fallback: String) -> String {
        guard let value else { return fallback }
        return resolvedSavedValue(value, fallback: fallback)
    }

    private static func runConfigurationCommand(
        _ action: ConfigurationAction,
        input: Data? = nil
    ) throws -> Data {
        // Process 的 stdout/stderr 和可选 stdin 都在同一调用中收束；非零退出统一转为 CoreCredentialError。
        let process = Process()
        process.executableURL = try CoreRuntimePaths.executableURL()
        process.arguments = [
            "observatory-config",
            "--config", try CoreRuntimePaths.configURL().path,
            action.rawValue,
        ]
        let stdout = Pipe()
        let stderr = Pipe()
        process.standardOutput = stdout
        process.standardError = stderr
        let stdin = input.map { _ in Pipe() }
        process.standardInput = stdin
        try process.run()
        if let input, let stdin {
            stdin.fileHandleForWriting.write(input)
            try stdin.fileHandleForWriting.close()
        }
        process.waitUntilExit()
        let output = stdout.fileHandleForReading.readDataToEndOfFile()
        let error = stderr.fileHandleForReading.readDataToEndOfFile()
        guard process.terminationStatus == 0 else {
            let message = String(data: error, encoding: .utf8)?
                .trimmingCharacters(in: .whitespacesAndNewlines)
            throw CoreCredentialError.configuration(
                message.flatMap { $0.isEmpty ? nil : $0 }
                    ?? "core exited with status \(process.terminationStatus)"
            )
        }
        return output
    }

    private static func environmentDraft() -> CoreConfigurationDraft {
        // 环境变量只生成初始 Draft；JSON route 解码失败时保持默认 route，而不是猜测配置含义。
        let environment = ProcessInfo.processInfo.environment
        var draft = CoreConfigurationDraft()
        draft.llmBaseURL = environment["LLM_GATEWAY_BASE_URL"] ?? ""
        draft.llmAPIKey = environment["LLM_GATEWAY_API_KEY"] ?? ""
        draft.globalModel = environment["AKZIO_MODEL"] ?? draft.globalModel
        draft.globalReasoningEffort = environment["AKZIO_REASONING_EFFORT"]
            ?? draft.globalReasoningEffort
        draft.globalResponseLanguage = environment["AKZIO_RESPONSE_LANGUAGE"]
            ?? draft.globalResponseLanguage
        if let value = environment["AKZIO_MODEL_ROUTES_JSON"],
           let data = value.data(using: .utf8),
           let routes = try? JSONDecoder().decode(
               [CoreModelStage: CoreStageModelConfiguration].self,
               from: data
           )
        {
            draft.stageModels.merge(routes) { _, environmentValue in environmentValue }
        }
        draft.alpacaAPIKey = environment["ALPACA_API_KEY"] ?? ""
        draft.alpacaAPISecret = environment["ALPACA_API_SECRET"] ?? ""
        draft.fredAPIKey = environment["FRED_API_KEY"] ?? ""
        draft.secUserAgent = environment["SEC_USER_AGENT"] ?? ""
        return draft
    }

    private static func hasValue(_ value: String?) -> Bool {
        !(value?.isEmpty ?? true)
    }
}
