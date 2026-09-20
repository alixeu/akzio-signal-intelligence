import Foundation

// The Core owns every eligibility flag. Unknown business payloads stay inspectable
// without making the Swift layer another workflow or contract implementation.
struct DebugSessionPayload: Decodable, Sendable {
    let identity: JSONValue
    let revision: UInt64
    let status: String
    let execution_mode: String
    let updated_at: Date
    var runID: String { identity["run_id"]?.string ?? "" }
}

struct DebugNodePayload: Decodable, Identifiable, Sendable {
    let task: JSONValue
    let role: String
    let horizon: String?
    let attempts: [JSONValue]
    let business_ready: Bool
    let step_eligible: Bool
    let retry_eligible: Bool
    let blocked_reason: String?
    let output_refs: [JSONValue]
    let budget: JSONValue
    var id: String { task["node"]?["task_id"]?.string ?? "" }
    var status: String { task["status"]?.string ?? "unknown" }
}

struct DebugRunPayload: Decodable, Sendable {
    let inspection: RuntimeInspectionPayload?
    let research: JSONValue?
    let session: DebugSessionPayload
    let workflow_status: String
    let execution_evidence: String?
    let nodes: [DebugNodePayload]
    let events: [JSONValue]
    let artifacts: [JSONValue]
    let acceptance: [JSONValue]
    let observed_at: Date
    let allowed_actions: [String]
}

struct DebugRunList: Decodable, Sendable {
    struct Entry: Decodable, Identifiable, Sendable {
        let session: DebugSessionPayload
        let lifecycle: JSONValue
        var id: String { session.runID }
    }
    let enabled: Bool
    let store_identity: String?
    let runs: [Entry]
}

extension ObserverClient {
    func debugRuns() async throws -> DebugRunList {
        try await debugRequest(path: "v1/debug/runs")
    }

    func debugRun(_ id: String) async throws -> DebugRunPayload {
        try await debugRequest(path: "v1/debug/runs/\(id)")
    }

    func debugControl(run: String, action: String, revision: UInt64, task: String?) async throws -> DebugSessionPayload {
        var body: [String: Any] = ["action": action, "expected_revision": revision]
        if let task { body["task_id"] = task }
        return try await debugRequest(path: "v1/debug/runs/\(run)/control", body: body)
    }

    func debugPrepare(session: String, purpose: String) async throws -> DebugSessionPayload {
        try await debugRequest(path: "v1/debug/runs", body: ["session_key": session, "purpose": purpose, "paper_allowed": false])
    }

    func debugFork(run: String, task: String?, reason: String, experimentID: String) async throws -> DebugSessionPayload {
        var body: [String: Any] = ["reason": reason, "experiment_id": experimentID]
        if let task { body["task_id"] = task }
        return try await debugRequest(path: "v1/debug/runs/\(run)/fork", body: body)
    }

    private func debugRequest<T: Decodable>(path: String, body: [String: Any]? = nil) async throws -> T {
        var request = URLRequest(url: endpoint.appending(path: path))
        request.setValue(token, forHTTPHeaderField: "x-akzio-token")
        request.timeoutInterval = ObserverTransportPolicy.standardRequestTimeout
        if let body {
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
        }
        let (data, response) = try await ObserverTransportPolicy.session.data(for: request)
        guard let response = response as? HTTPURLResponse else { throw ObserverClientError.invalidResponse }
        guard response.statusCode == 200 else {
            // Debug errors are Core-generated, credential-free diagnostics.
            let message = (try? JSONSerialization.jsonObject(with: data) as? [String: String])?["error"]
            throw DebugAPIError(message: message ?? "HTTP \(response.statusCode)")
        }
        return try Self.decoder().decode(T.self, from: data)
    }
}

struct DebugAPIError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}
