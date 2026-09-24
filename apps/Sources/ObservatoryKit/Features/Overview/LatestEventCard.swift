import SwiftUI

// 文件导读：Overview 右栏按上游顺序展示最新事件和最多四条补充事件，更多记录由本地
// DisclosureGroup 展开；空数组明确显示 queued 占位。读 body、localizedDetail，关注
// State 只控制 UI 展开/一次性脉冲，事件内容与严重级别仍来自 EventPresentation。
// MARK: - Latest event
//
// When a new event arrives the border pulses grey → coral → neutral exactly once.
// It never keeps flashing: a persistent alarm stops being information.
struct LatestEventCard: View {
    // events 按最新优先传入；卡片只负责展开最近事件和最多五条补充事件。
    let events: [EventPresentation]

    @Environment(\.motionPolicy) private var policy
    @Environment(\.appLanguage) private var language
    // pulseTick 是一次性动画触发器，expanded 只控制更多历史事件的本地展开。
    @State private var pulseTick = 0
    @State private var expanded = false

    private var latest: EventPresentation? { events.first }

    var body: some View {
        // latest 变化触发一次边框脉冲；事件内容本身不通过动画状态重新生成。
        SectionCard(title: "Latest Event", subtitle: latest?.relativeLabel) {
            if let latest {
                VStack(alignment: .leading, spacing: AkzioLayout.s2) {
                    HStack(spacing: AkzioLayout.s2) {
                        Image(systemName: latest.symbol)
                            .font(.system(size: 13, weight: .medium))
                            .foregroundStyle(latest.severity.tone.color)
                        Text(L10n.text(latest.title, language: language)).akzioText(.sectionTitle)
                        Spacer(minLength: AkzioLayout.s2)
                    PillTag(L10n.text(latest.severity.label, language: language), tone: latest.severity.tone)
                    }
                Text(localizedDetail(latest.detail)).akzioText(.bodySmall)
                    if events.count > 1 {
                        // events 已按新到旧排序；dropFirst 跳过主卡，prefix(4) 限制次级列表长度。
                        HairlineDivider()
                        VStack(alignment: .leading, spacing: 5) {
                            ForEach(Array(events.dropFirst().prefix(4).enumerated()), id: \.element.id) { index, event in
                                HStack(spacing: AkzioLayout.s2) {
                                    Circle()
                                        .fill(event.severity.tone.color.opacity(0.7))
                                        .frame(width: 5, height: 5)
                                    Text(L10n.text(event.title, language: language)).akzioText(.bodySmall)
                                    Spacer(minLength: AkzioLayout.s2)
                                    Text(L10n.text(event.relativeLabel, language: language)).akzioMono(10, color: AkzioColor.mutedText)
                                }
                                .staggeredReveal(index: index)
                            }
                        }
                    }
                }
                if events.count > 5 {
                    // 更多记录不是删除或分页请求，只在本地展开其余事件；expanded 由 DisclosureGroup 的 Binding 更新。
                    DisclosureGroup("更多事件（\(events.count - 5)）", isExpanded: $expanded) {
                        ScrollView {
                            LazyVStack(alignment: .leading, spacing: 10) {
                                ForEach(Array(events.dropFirst(5))) { event in
                                    HStack(alignment: .top) {
                                        Text(L10n.text(event.title, language: language)).akzioText(.bodySmall)
                                        Spacer(minLength: 4)
                                        Text(event.relativeLabel).akzioMono(11)
                                    }
                                }
                            }.padding(.vertical, 8)
                        }.frame(height: 160)
                    }.font(.callout)
                }
            } else {
                // 空数组表示当前没有已记录事件，不把它解读为失败或后台仍在运行。
                StatusExplanation(.queued, detail: "No events recorded for this run yet")
            }
        }
        .overlay {
            // One-shot border pulse, retriggered only when the newest event changes.
            RoundedRectangle(cornerRadius: AkzioLayout.cardRadius, style: .continuous)
                .strokeBorder(AkzioColor.actionCoral, lineWidth: 1.4)
                .opacity(0)
                .phaseAnimator([0, 1, 2], trigger: pulseTick) { view, step in
                    view.opacity(step == 1 ? 0.9 : 0)
                } animation: { step in
                    policy.resolve(.easeOut(duration: step == 1 ? 0.22 : 0.42))
                }
                .allowsHitTesting(false)
        }
        .onChange(of: latest?.id) { _, _ in pulseTick += 1 }
    }

    private func localizedDetail(_ detail: String) -> String {
        // 任务前缀单独翻译，其余详情交给 L10n；这里不改变事件原文。
        guard detail.hasPrefix("Task ") else {
            return L10n.text(detail, language: language)
        }
        return "\(L10n.text("Task", language: language)) \(detail.dropFirst(5))"
    }
}
