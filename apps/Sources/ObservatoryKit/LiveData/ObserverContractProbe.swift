import Foundation

public struct ObserverContractSummary: Sendable, Equatable {
    public let schemaVersion: Int
    public let eventCursor: Int64
    public let runCount: Int
    public let firstRunID: String?
    public let readinessPpm: Int?
    public let outcomeStatus: String?
}

public struct ObserverTraceContractSummary: Sendable, Equatable {
    public let artifactID: String?
    public let toolCallID: String?
    public let toolName: String?
    public let toolLifecycle: String?
    public let outputReferenceIDs: [String]
    public let structuredArtifactKinds: [String]
    public let firstStructuredPayload: String?
}

public struct ObserverReasoningContractSummary: Sendable, Equatable {
    public let type: String
    public let runID: String
    public let taskID: String
    public let purpose: String
    public let turn: UInt16
    public let delta: String?
}

public enum ObserverContractProbe {
    public static func decode(_ data: Data) throws -> ObserverContractSummary {
        let payload = try ObserverClient.decoder().decode(ObserverSnapshotPayload.self, from: data)
        return ObserverContractSummary(
            schemaVersion: payload.schemaVersion,
            eventCursor: payload.eventCursor,
            runCount: payload.recentRuns.count,
            firstRunID: payload.recentRuns.first?.run.runID,
            readinessPpm: payload.core.readinessPpm.map(Int.init),
            outcomeStatus: payload.outcome?.status
        )
    }

    public static func decodeTrace(_ data: Data) throws -> ObserverTraceContractSummary {
        let payload = try ObserverClient.decoder().decode(ObserverTraceEnvelope.self, from: data)
        let first = payload.trajectory.first
        return ObserverTraceContractSummary(
            artifactID: first?.artifactID,
            toolCallID: first?.tool?.callID,
            toolName: first?.tool?.name,
            toolLifecycle: first?.tool?.lifecycle,
            outputReferenceIDs: first?.outputRefs?.map(\.artifactID) ?? [],
            structuredArtifactKinds: payload.artifacts.map(\.kind),
            firstStructuredPayload: payload.artifacts.first?.payload.prettyPrinted
        )
    }

    public static func decodeReasoning(_ data: Data) throws -> ObserverReasoningContractSummary {
        let payload = try ObserverClient.decoder().decode(
            ObserverReasoningEventPayload.self,
            from: data
        )
        return ObserverReasoningContractSummary(
            type: payload.type,
            runID: payload.runID,
            taskID: payload.taskID,
            purpose: payload.purpose,
            turn: payload.turn,
            delta: payload.delta
        )
    }
}

private struct ObserverTraceEnvelope: Decodable {
    let trajectory: [ObserverTrajectoryPayload]
    let artifacts: [ObserverArtifactPayload]
}
