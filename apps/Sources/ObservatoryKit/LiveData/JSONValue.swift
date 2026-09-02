import Foundation

enum JSONValue: Decodable, Sendable {
    case object([String: JSONValue])
    case array([JSONValue])
    case string(String)
    case number(Double)
    case bool(Bool)
    case null

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() { self = .null }
        else if let value = try? container.decode(Bool.self) { self = .bool(value) }
        else if let value = try? container.decode(Double.self) { self = .number(value) }
        else if let value = try? container.decode(String.self) { self = .string(value) }
        else if let value = try? container.decode([JSONValue].self) { self = .array(value) }
        else { self = .object(try container.decode([String: JSONValue].self)) }
    }

    subscript(_ key: String) -> JSONValue? {
        guard case .object(let values) = self else { return nil }
        return values[key]
    }

    var string: String? {
        guard case .string(let value) = self else { return nil }
        return value
    }

    var int: Int? {
        guard case .number(let value) = self else { return nil }
        return Int(exactly: value)
    }

    var int64: Int64? {
        guard case .number(let value) = self else { return nil }
        return Int64(exactly: value)
    }

    var array: [JSONValue]? {
        guard case .array(let value) = self else { return nil }
        return value
    }

    var object: [String: JSONValue]? {
        guard case .object(let value) = self else { return nil }
        return value
    }

    var bool: Bool? {
        guard case .bool(let value) = self else { return nil }
        return value
    }

    var prettyPrinted: String {
        let object = foundationObject
        guard JSONSerialization.isValidJSONObject(object),
              let data = try? JSONSerialization.data(
                  withJSONObject: object,
                  options: [.prettyPrinted, .sortedKeys]
              ),
              let value = String(data: data, encoding: .utf8)
        else {
            return String(describing: object)
        }
        return value
    }

    private var foundationObject: Any {
        switch self {
        case .object(let values):
            values.mapValues(\.foundationObject)
        case .array(let values):
            values.map(\.foundationObject)
        case .string(let value):
            value
        case .number(let value):
            value
        case .bool(let value):
            value
        case .null:
            NSNull()
        }
    }
}
