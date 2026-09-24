import Darwin
import Foundation
import Observation
import Security

// 状态是可跨并发边界传递的值语义枚举；failed 单独携带本地化诊断，其余 case 不保存可变进程对象。
public enum RustCoreState: Sendable, Equatable {
    case stopped
    case needsConfiguration
    case starting
    case waitingReady
    case ready
    case failed(String)
    case stopping

    public var label: String {
        // label 只把状态映射成稳定 UI 文案，不暴露错误详情或改变状态。
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
        // 只有 failed 携带 detail，其他状态返回 nil 让 UI 保持明确的无诊断边界。
        guard case .failed(let message) = self else { return nil }
        return message
    }
}

// Connection 是启动完成后的不可变 endpoint/token 快照；传给客户端时不会共享 supervisor 的可变字段。
public struct RustCoreConnection: Sendable, Equatable {
    public let endpoint: URL
    public let controlToken: String
}

@MainActor
@Observable
public final class RustCoreSupervisor {
    // Supervisor 由 MainActor 串行管理 UI 状态和 Process 引用；异步网络/等待通过 await 返回主 actor 更新。
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
        // 已运行时直接复用 connection；nil 表示配置、启动或 ready 流程没有完成。
        if process?.isRunning == true { return connection }
        do {
            // CredentialStore 是启动前的认证边界；没有配置只进入 needsConfiguration，不启动本地进程。
            guard let configuration = try CoreCredentialStore.resolved() else {
                state = .needsConfiguration
                return nil
            }
            state = .starting
            recentOutput = ""
            // executable/config/store 都由受控路径解析；storePath 只供 UI 观察，Store 目录由 Core 使用。
            let executable = try CoreRuntimePaths.executableURL()
            let config = try CoreRuntimePaths.configURL()
            let store = try CoreRuntimePaths.storeURL()
            storePath = store.path
            try openLog()
            appendOutput("\n=== Akzio Core start \(Date().formatted(.iso8601)) ===\n")
            let endpoint = URL(string: "http://127.0.0.1:7342")!
            // 端口占用在启动前拒绝，避免把其他进程误认成当前 Core。
            if await endpointIsOccupied(endpoint) {
                throw CoreLaunchError.portOccupied
            }

            // token 与 Process pipe 一起创建；子进程只接收隔离后的环境变量，不共享 Swift 对象。
            let controlToken = try loadOrCreateControlToken(store: store)
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
                store: store
            )
            attachOutput(stdoutPipe.fileHandleForReading)
            attachOutput(stderrPipe.fileHandleForReading)
            expectedStop = false
            // terminationHandler 是异步闭包：弱引用 supervisor，并回到 MainActor 更新状态，防止进程退出后悬挂引用。
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

            // 先等待带 token 的 /ready，再通过 ObserverClient fetchSnapshot 验证 HTTP 连接确实可用。
            try await waitUntilReady(
                endpoint: endpoint,
                controlToken: controlToken,
                process: process
            )
            let client = try ObserverClient(endpoint: endpoint, token: controlToken)
            _ = try await client.fetchSnapshot()
            let connection = RustCoreConnection(endpoint: endpoint, controlToken: controlToken)
            self.connection = connection
            self.controlToken = controlToken
            state = .ready
            return connection
        } catch {
            // 任一配置、Process、ready 或 snapshot 错误都回收资源并进入 failed，不返回半初始化 connection。
            stop()
            state = .failed(error.localizedDescription)
            return nil
        }
    }

    public func restart() async -> RustCoreConnection? {
        // restart 复用同一 stop/start 生命周期，确保旧进程、pipe、token 引用先被清理。
        stop()
        return await start()
    }

    public func stop() {
        // stop 可重复调用；没有 Process 时仍关闭日志并回到 stopped。
        guard let process else {
            closeLog()
            state = .stopped
            return
        }
        expectedStop = true
        state = .stopping
        // 先关闭 stdin 并请求优雅退出，最多等待五秒，超时才使用 SIGKILL。
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
        // 清空所有引用和 readabilityHandler，避免旧进程输出继续写入当前 supervisor。
        closeLog()
        state = .stopped
    }

    public func submitRun(purpose: RunPurpose) async throws -> String {
        // 只有 userLaunchModes 可由 App 提交；状态/connection/token 任一缺失都在 HTTP I/O 前抛出 notReady。
        guard RunPurpose.userLaunchModes.contains(purpose) else {
            throw CoreLaunchError.runRejected
        }
        guard state == .ready,
              let connection,
              let controlToken
        else { throw CoreLaunchError.notReady }
        var request = URLRequest(url: connection.endpoint.appending(path: "runs"))
        request.httpMethod = "POST"
        request.setValue(controlToken, forHTTPHeaderField: "x-akzio-token")
        request.setValue("application/json", forHTTPHeaderField: "content-type")
        request.httpBody = try JSONEncoder().encode(RunSubmissionRequest(purpose: purpose))
        request.timeoutInterval = 60
        // 请求带 x-akzio-token 认证并使用受控 session；非 2xx 先尝试解析 Core 的拒绝原因。
        let (data, response) = try await ObserverTransportPolicy.session.data(for: request)
        guard let response = response as? HTTPURLResponse,
              (200..<300).contains(response.statusCode)
        else {
            let detail = (try? JSONDecoder().decode(RunRejection.self, from: data))?.error
            throw CoreLaunchError.runFailure(detail ?? "Rust Core 拒绝运行请求")
        }
        return try JSONDecoder().decode(RunSubmission.self, from: data).runID
    }

    private func waitUntilReady(
        endpoint: URL,
        controlToken: String,
        process: Process
    ) async throws {
        // ready 轮询最多五分钟；每次请求携带 control token，进程退出或超时都保留明确错误。
        let deadline = ContinuousClock.now.advanced(by: .seconds(300))
        while ContinuousClock.now < deadline {
            guard process.isRunning else { throw CoreLaunchError.exitedBeforeReady }
            var request = URLRequest(url: endpoint.appending(path: "ready"))
            request.setValue(controlToken, forHTTPHeaderField: "x-akzio-token")
            request.timeoutInterval = 1
            if let (_, response) = try? await ObserverTransportPolicy.session.data(for: request),
               (response as? HTTPURLResponse)?.statusCode == 200
            {
                return
            }
            // Task.sleep 只暂停当前 async task，不阻塞 MainActor 的 UI 运行循环。
            try await Task.sleep(for: .milliseconds(200))
        }
        throw CoreLaunchError.readyTimeout
    }

    private func endpointIsOccupied(_ endpoint: URL) async -> Bool {
        // 这是启动前的只读探测；任何可连接响应都视为端口已被占用，不尝试控制未知进程。
        var request = URLRequest(url: endpoint.appending(path: "ready"))
        request.timeoutInterval = 0.5
        return (try? await ObserverTransportPolicy.session.data(for: request)) != nil
    }

    private func childEnvironment(
        configuration: CoreConfiguration,
        store: URL
    ) -> [String: String] {
        // 子进程环境以当前父环境为基础，只注入 Core store、Paper endpoint、模型路由和认证配置。
        var environment = ProcessInfo.processInfo.environment
        environment["AKZIO_STORE_ROOT"] = store.path
        environment["AKZIO_EXIT_ON_STDIN_EOF"] = "1"
        environment["LLM_GATEWAY_BASE_URL"] = configuration.llmBaseURL
        environment["LLM_GATEWAY_API_KEY"] = configuration.llmAPIKey
        environment["AKZIO_MODEL"] = configuration.globalModel
        environment["AKZIO_REASONING_EFFORT"] = configuration.globalReasoningEffort
        environment["AKZIO_RESPONSE_LANGUAGE"] = configuration.globalResponseLanguage
        let routes = Dictionary(uniqueKeysWithValues: configuration.stageModels.map {
            ($0.key.rawValue, $0.value)
        })
        // 路由字典编码失败时不写入 routes 变量；其他已验证配置仍会传给子进程。
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

    private func loadOrCreateControlToken(store: URL) throws -> String {
        // token 文件位于本次 Core store 下；已有 token 必须是非空单行 UTF-8，并强制回写 0600 权限。
        let fileManager = FileManager.default
        try fileManager.createDirectory(at: store, withIntermediateDirectories: true)
        let tokenURL = store.appendingPathComponent(".daemon-token", isDirectory: false)
        if fileManager.fileExists(atPath: tokenURL.path) {
            let data = try Data(contentsOf: tokenURL)
            guard let token = String(data: data, encoding: .utf8),
                !token.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                !token.contains("\n"),
                !token.contains("\r")
            else {
                throw CoreCredentialError.configuration("daemon token file is invalid")
            }
            try fileManager.setAttributes(
                [.posixPermissions: NSNumber(value: Int16(0o600))],
                ofItemAtPath: tokenURL.path
            )
            return token
        }

        // 没有合法旧 token 时用系统安全随机源创建一次，再以 atomic 写入并收紧权限。
        let token = try randomToken()
        try Data(token.utf8).write(to: tokenURL, options: .atomic)
        try fileManager.setAttributes(
            [.posixPermissions: NSNumber(value: Int16(0o600))],
            ofItemAtPath: tokenURL.path
        )
        return token
    }

    private func randomToken() throws -> String {
        // SecRandomCopyBytes 失败即抛错；随机字节转成固定长度 hex，不使用可预测的时间/进程信息。
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
        // readabilityHandler 捕获 weak self；availableData 的异步回调把 UTF-8 文本交回 MainActor，空数据表示 EOF。
        handle.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let text = String(data: data, encoding: .utf8) else { return }
            Task { @MainActor in self?.appendOutput(text) }
        }
    }

    private func appendOutput(_ text: String) {
        // 日志同时写入 0600 文件和内存环形尾部；recentOutput 限制为最后 8,192 个 UTF-8 字节附近的字符窗口。
        if let data = text.data(using: .utf8) {
            try? logHandle?.write(contentsOf: data)
        }
        recentOutput.append(text)
        if recentOutput.utf8.count > 8_192 {
            recentOutput = String(recentOutput.suffix(8_192))
        }
    }

    private func openLog() throws {
        // 日志文件由受控运行时路径创建，随后设置 0600 并 seek 到末尾，保留本次 Core 的追加历史。
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
        // closeLog 是幂等清理，忽略关闭失败但清空 handle，避免后续输出写到旧文件句柄。
        try? logHandle?.close()
        logHandle = nil
    }
}

// 启动/控制错误保持为值语义枚举，供 UI 显示稳定 description 而不泄露 token 或完整请求体。
enum CoreLaunchError: LocalizedError {
    case missingConfiguration
    case missingExecutable
    case missingConfig
    case portOccupied
    case exitedBeforeReady
    case readyTimeout
    case notReady
    case runRejected
    case runFailure(String)

    var errorDescription: String? {
        // errorDescription 只提供用户可读原因；具体认证材料和 provider 响应不在这里回显。
        switch self {
        case .missingConfiguration: "Core credentials are not configured"
        case .missingExecutable: "Bundled Rust core executable was not found"
        case .missingConfig: "Bundled Rust core configuration was not found"
        case .portOccupied: "127.0.0.1:7342 is already occupied by another process"
        case .exitedBeforeReady: "Rust core exited before becoming ready"
        case .readyTimeout: "Core 在 5 分钟内未就绪，请检查模型连接和 Core 日志"
        case .notReady: "Rust core is not ready"
        case .runRejected: "Rust core rejected the run"
        case .runFailure(let message): message
        }
    }
}

private struct RunSubmission: Decodable {
    // Rust 返回的 run_id 通过 CodingKeys 从协议字段映射到 Swift 属性。
    let runID: String

    enum CodingKeys: String, CodingKey {
        case runID = "run_id"
    }
}

private struct RunSubmissionRequest: Encodable {
    // 提交请求只编码受控 RunPurpose，不能从 UI 字符串拼接任意命令。
    let purpose: RunPurpose
}

// 非 2xx 的最小错误 envelope，只读取 Core 提供的 error 文本。
private struct RunRejection: Decodable { let error: String }
