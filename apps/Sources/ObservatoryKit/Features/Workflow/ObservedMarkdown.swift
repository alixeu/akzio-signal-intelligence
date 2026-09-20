import SwiftUI

/// Native Markdown for Observer-approved text. No remote resources are fetched.
/// Copying is handled by the record row and preserves its untouched source.
struct ObservedMarkdown: View {
    let source: String

    private struct Block: Identifiable {
        let id: Int
        var text: AttributedString
        let components: [PresentationIntent.IntentType]

        var heading: Int? {
            for component in components {
                if case .header(let level) = component.kind { return level }
            }
            return nil
        }
        var marker: String? {
            for component in components {
                if case .listItem(let ordinal) = component.kind {
                    let ordered = components.contains { if case .orderedList = $0.kind { return true }; return false }
                    return ordered ? "\(ordinal)." : "•"
                }
            }
            return nil
        }
        var isCode: Bool {
            components.contains { if case .codeBlock = $0.kind { return true }; return false }
        }
    }

    private var blocks: [Block] {
        guard let parsed = try? AttributedString(markdown: source) else {
            return [Block(id: 0, text: AttributedString(source), components: [])]
        }
        var result: [Block] = []
        for run in parsed.runs {
            let components = run.presentationIntent?.components ?? []
            let id = components.first?.identity ?? 0
            let text = AttributedString(parsed[run.range])
            if result.last?.id == id {
                result[result.count - 1].text.append(text)
            } else {
                result.append(Block(id: id, text: text, components: components))
            }
        }
        return result
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            ForEach(blocks) { block in
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    if let marker = block.marker { Text(marker).akzioText(.body) }
                    Text(block.text)
                        .font(block.isCode ? .system(size: 12, design: .monospaced) : .system(size: block.heading == nil ? 13 : 15))
                        .fontWeight(block.heading == nil ? .regular : .semibold)
                        .foregroundStyle(AkzioColor.primaryText)
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
        }
    }
}
