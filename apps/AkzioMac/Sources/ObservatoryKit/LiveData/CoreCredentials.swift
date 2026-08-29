import Foundation

public enum CoreModelStage: String, CaseIterable, Codable, Sendable, Hashable, Identifiable {
    case planner = "research.planner"
    case analyst = "research.analyst"
    case critic = "research.critic"
    case synthesizer = "research.synthesizer"
    case outcomeReviewer = "learning.outcome_worker"

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .planner: "Planner"
        case .analyst: "Analyst"
        case .critic: "Critic"
        case .synthesizer: "Synthesizer"
        case .outcomeReviewer: "Outcome Reviewer"
        }
    }
}

public struct CoreStageModelConfiguration: Codable, Sendable, Equatable {
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
        !llmBaseURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
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

    public var requiredComplete: Bool {
        llmAPIKey && alpacaAPIKey && alpacaAPISecret && fredAPIKey
    }
}

public struct CoreConfigurationDraft: Sendable, Equatable {
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
            "LLM URL, LLM API key, Alpaca API key, Alpaca API secret, and FRED API key are required"
        }
    }
}

enum CoreRuntimePaths {
    static func executableURL() throws -> URL {
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

    static func configURL() throws -> URL {
        let environment = ProcessInfo.processInfo.environment
        if let path = environment["AKZIO_CORE_CONFIG"], !path.isEmpty {
            return URL(fileURLWithPath: path)
        }
        let config = try homeURL().appending(path: "config.toml")
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
        let container = try decoder.container(keyedBy: CodingKeys.self)
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
        var container = encoder.container(keyedBy: CodingKeys.self)
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
    private enum ConfigurationAction: String {
        case get
        case set
    }

    static func status() -> CoreCredentialStatus {
        let draft = savedDraft()
        return CoreCredentialStatus(
            llmAPIKey: hasValue(draft.llmAPIKey),
            alpacaAPIKey: hasValue(draft.alpacaAPIKey),
            alpacaAPISecret: hasValue(draft.alpacaAPISecret),
            fredAPIKey: hasValue(draft.fredAPIKey)
        )
    }

    static func savedDraft() -> CoreConfigurationDraft {
        (try? fileDraft()) ?? environmentDraft()
    }

    static func resolved() throws -> CoreConfiguration? {
        let draft = try fileDraft()
        let configuration = CoreConfiguration(
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
        let configuration = CoreConfiguration(
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
        var draft = try fileDraft()
        draft.llmAPIKey = ""
        draft.alpacaAPIKey = ""
        draft.alpacaAPISecret = ""
        draft.fredAPIKey = ""
        let configuration = CoreFileConfiguration(
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
        let data = try runConfigurationCommand(.get)
        let configuration = try JSONDecoder().decode(CoreFileConfiguration.self, from: data)
        var draft = environmentDraft()
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
