import SwiftUI

/// Native Markdown for Observer-approved text. No remote resources are fetched.
/// Copying is handled by the record row and preserves its untouched source.
struct ObservedMarkdown: View {
    let source: String

    private struct Block: Identifiable {
        // Block 是本地 Markdown 解析后的展示片段；id 来自 PresentationIntent，组件信息
        // 只用于决定标题、列表标记和等宽代码样式，不携带原始 provider 隐藏内容。
        let id: Int
        var text: AttributedString
        let components: [PresentationIntent.IntentType]

        var heading: Int? {
            // 取首个 header 层级；没有 header 时保持 nil，由 body 选择普通字号。
            for component in components {
                if case .header(let level) = component.kind { return level }
            }
            return nil
        }
        var marker: String? {
            // 只有 listItem 才显示标记；同一片段包含 orderedList 时使用序号，否则使用圆点。
            for component in components {
                if case .listItem(let ordinal) = component.kind {
                    let ordered = components.contains { if case .orderedList = $0.kind { return true }; return false }
                    return ordered ? "\(ordinal)." : "•"
                }
            }
            return nil
        }
        var isCode: Bool {
            // codeBlock 只改变字体，不把代码内容执行或送回任何服务。
            components.contains { if case .codeBlock = $0.kind { return true }; return false }
        }
    }

    private var blocks: [Block] {
        // 这是 computed projection：每次 source 改变时重新解析。解析失败通过 try? 回退到
        // 一段普通文本，保证展示仍可见，但不宣称输入符合 Markdown 语法。
        guard let parsed = try? AttributedString(markdown: source) else {
            return [Block(id: 0, text: AttributedString(source), components: [])]
        }
        var result: [Block] = []
        for run in parsed.runs {
            // 相邻 run 若属于同一 presentation identity 就合并，避免把一个视觉段落拆成
            // 多个 View；这里仍只处理 source 的本地 AttributedString。
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
        // ForEach 只渲染已解析的 blocks；textSelection 是本地复制/选择能力，远端资源不会
        // 被 Markdown 自动加载。
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
