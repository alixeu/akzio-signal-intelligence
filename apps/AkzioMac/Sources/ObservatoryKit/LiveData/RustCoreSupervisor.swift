import Darwin
import Foundation
import Observation
import Security

public enum RustCoreState: Sendable, Equatable {
    case stopped
    case needsConfiguration
    case starting
    case waitingReady
    case ready
    case failed(String)
    case stopping

    public var label: String {
        switch self {
        case .stopped: "Stopped"
        case .needsConfiguration: "Needs Configuration"
        case .starting: "Starting"
        case .waitingReady: "Waiting for Ready"
        case .ready: "Ready"
        case .failed: "Failed"
        case .stopping: "Stopping"
        }
    }

    public var detail: String? {
        guard case .failed(let message) = self else { return nil }
        return message
    }
}

public struct RustCoreConnection: Sendable, Equatable {
    public let endpoint: URL
    public let observerToken: String
}

@MainActor
@Observable
public final class RustCoreSupervisor {
    public static let shared = RustCoreSupervisor()

    public private(set) var state: RustCoreState = .stopped
    public private(set) var storePath = ""
    public private(set) var recentOutput = ""

    private var process: Process?
    private var stdinPipe: Pipe?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    private var logHandle: FileHandle?
    private var connection: RustCoreConnection?
    private var controlToken: String?
    private var expectedStop = false

    private init() {}

    public func start() async -> RustCoreConnection? {
        if process?.isRunning == true { return connection }
        do {
            guard let configuration = try CoreCredentialStore.resolved() else {
                state = .needsConfiguration
                return nil
            }
            state = .starting
            recentOutput = ""
            let executable = try CoreRuntimePaths.executableURL()
            let config = try CoreRuntimePaths.configURL()
            let store = try CoreRuntimePaths.storeURL()
            storePath = store.path
            try openLog()
            appendOutput("\n=== Akzio Core start \(Date().formatted(.iso8601)) ===\n")
            let endpoint = URL(string: "http://127.0.0.1:7342")!
            if await endpointIsOccupied(endpoint) {
                throw CoreLaunchError.portOccupied
            }

            let controlToken = try randomToken()
            let observerToken = try randomToken()
            let process = Process()
            let stdinPipe = Pipe()
            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()
            process.executableURL = executable
            process.arguments = ["--config", config.path, "daemon", "serve"]
            process.currentDirectoryURL = config.deletingLastPathComponent()
            process.standardInput = stdinPipe
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe
            process.environment = childEnvironment(
                configuration: configuration,
                store: store,
                controlToken: controlToken,
                observerToken: observerToken
            )
            attachOutput(stdoutPipe.fileHandleForReading)
            attachOutput(stderrPipe.fileHandleForReading)
            expectedStop = false
            process.terminationHandler = { [weak self] process in
                Task { @MainActor in
                    guard let self else { return }
                    if !self.expectedStop {
                        self.state = .failed(
                            "Rust core exited with status \(process.terminationStatus)"
                        )
                    }
                    self.connection = nil
                }
            }
            try process.run()
            self.process = process
            self.stdinPipe = stdinPipe
            self.stdoutPipe = stdoutPipe
            self.stderrPipe = stderrPipe
            state = .waitingReady

            try await waitUntilReady(
                endpoint: endpoint,
                controlToken: controlToken,
                process: process
            )
            let client = try ObserverClient(endpoint: endpoint, token: observerToken)
            _ = try await client.fetchSnapshot()
            let connection = RustCoreConnection(endpoint: endpoint, observerToken: observerToken)
            self.connection = connection
            self.controlToken = controlToken
            state = .ready
            return connection
        } catch {
            stop()
            state = .failed(error.localizedDescription)
            return nil
        }
    }

    public func restart() async -> RustCoreConnection? {
        stop()
        return await start()
    }

    public func stop() {
        guard let process else {
            closeLog()
            state = .stopped
            return
        }
        expectedStop = true
        state = .stopping
        try? stdinPipe?.fileHandleForWriting.close()
        if process.isRunning { process.terminate() }
        let deadline = Date().addingTimeInterval(5)
        while process.isRunning && Date() < deadline {
            RunLoop.current.run(until: Date().addingTimeInterval(0.05))
        }
        if process.isRunning { Darwin.kill(process.processIdentifier, SIGKILL) }
        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil
        self.process = nil
        stdinPipe = nil
        stdoutPipe = nil
        stderrPipe = nil
        connection = nil
        controlToken = nil
        closeLog()
        state = .stopped
    }

    public func submitDebugRun() async throws -> String {
        guard state == .ready,
              let connection,
              let controlToken
        else { throw CoreLaunchError.notReady }
        var request = URLRequest(url: connection.endpoint.appending(path: "runs"))
        request.httpMethod = "POST"
        request.setValue(controlToken, forHTTPHeaderField: "x-akzio-token")
        request.setValue("application/json", forHTTPHeaderField: "content-type")
        request.httpBody = Data(#"{"purpose":"debug"}"#.utf8)
        request.timeoutInterval = 5
        let (data, response) = try await URLSession.shared.data(for: request)
        guard let response = response as? HTTPURLResponse,
              (200..<300).contains(response.statusCode)
        else { throw CoreLaunchError.runRejected }
        return try JSONDecoder().decode(RunSubmission.self, from: data).runID
    }

    private func waitUntilReady(
        endpoint: URL,
        controlToken: String,
        process: Process
    ) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(15))
        while ContinuousClock.now < deadline {
            guard process.isRunning else { throw CoreLaunchError.exitedBeforeReady }
            var request = URLRequest(url: endpoint.appending(path: "ready"))
            request.setValue(controlToken, forHTTPHeaderField: "x-akzio-token")
            request.timeoutInterval = 1
            if let (_, response) = try? await URLSession.shared.data(for: request),
               (response as? HTTPURLResponse)?.statusCode == 200
            {
                return
            }
            try await Task.sleep(for: .milliseconds(200))
        }
        throw CoreLaunchError.readyTimeout
    }

    private func endpointIsOccupied(_ endpoint: URL) async -> Bool {
        var request = URLRequest(url: endpoint.appending(path: "ready"))
        request.timeoutInterval = 0.5
        return (try? await URLSession.shared.data(for: request)) != nil
    }

    private func childEnvironment(
        configuration: CoreConfiguration,
        store: URL,
        controlToken: String,
        observerToken: String
    ) -> [String: String] {
        var environment = ProcessInfo.processInfo.environment
        environment["AKZIO_STORE_ROOT"] = store.path
        environment["AKZIO_DAEMON_TOKEN"] = controlToken
        environment["AKZIO_OBSERVER_TOKEN"] = observerToken
        environment["AKZIO_EXIT_ON_STDIN_EOF"] = "1"
        environment["LLM_GATEWAY_BASE_URL"] = configuration.llmBaseURL
        environment["LLM_GATEWAY_API_KEY"] = configuration.llmAPIKey
        environment["AKZIO_MODEL"] = configuration.globalModel
        environment["AKZIO_REASONING_EFFORT"] = configuration.globalReasoningEffort
        environment["AKZIO_RESPONSE_LANGUAGE"] = configuration.globalResponseLanguage
        let routes = Dictionary(uniqueKeysWithValues: configuration.stageModels.map {
            ($0.key.rawValue, $0.value)
        })
        if let data = try? JSONEncoder().encode(routes),
           let value = String(data: data, encoding: .utf8) {
            environment["AKZIO_MODEL_ROUTES_JSON"] = value
        }
        environment["ALPACA_API_KEY"] = configuration.alpacaAPIKey
        environment["ALPACA_API_SECRET"] = configuration.alpacaAPISecret
        environment["ALPACA_PAPER_BASE_URL"] = "https://paper-api.alpaca.markets"
        if let value = configuration.fredAPIKey, !value.isEmpty {
            environment["FRED_API_KEY"] = value
        }
        if let value = configuration.secUserAgent, !value.isEmpty {
            environment["SEC_USER_AGENT"] = value
        }
        return environment
    }

    private func randomToken() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        let status = bytes.withUnsafeMutableBytes { buffer in
            SecRandomCopyBytes(kSecRandomDefault, buffer.count, buffer.baseAddress!)
        }
        guard status == errSecSuccess else {
            throw CoreCredentialError.configuration(
                "secure random generation failed (OSStatus \(status))"
            )
        }
        return bytes.map { String(format: "%02x", $0) }.joined()
    }

    private func attachOutput(_ handle: FileHandle) {
        handle.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }
            Task { @MainActor in self?.appendOutput(text) }
        }
    }

    private func appendOutput(_ text: String) {
        if let data = text.data(using: .utf8) {
            try? logHandle?.write(contentsOf: data)
        }
        recentOutput.append(text)
        if recentOutput.utf8.count > 8_192 {
            recentOutput = String(recentOutput.suffix(8_192))
        }
    }

    private func openLog() throws {
        let url = try CoreRuntimePaths.logURL()
        if !FileManager.default.fileExists(atPath: url.path) {
            FileManager.default.createFile(atPath: url.path, contents: nil)
        }
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600],
            ofItemAtPath: url.path
        )
        let handle = try FileHandle(forWritingTo: url)
        try handle.seekToEnd()
        logHandle = handle
    }

    private func closeLog() {
        try? logHandle?.close()
        logHandle = nil
    }
}

enum CoreLaunchError: LocalizedError {
    case missingConfiguration
    case missingExecutable
    case missingConfig
    case portOccupied
    case exitedBeforeReady
    case readyTimeout
    case notReady
    case runRejected

    var errorDescription: String? {
        switch self {
        case .missingConfiguration: "Core credentials are not configured"
        case .missingExecutable: "Bundled Rust core executable was not found"
        case .missingConfig: "Bundled Rust core configuration was not found"
        case .portOccupied: "127.0.0.1:7342 is already occupied by another process"
        case .exitedBeforeReady: "Rust core exited before becoming ready"
        case .readyTimeout: "Rust core did not become ready within 15 seconds"
        case .notReady: "Rust core is not ready"
        case .runRejected: "Rust core rejected the Debug run"
        }
    }
}

private struct RunSubmission: Decodable {
    let runID: String

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
    }
}
