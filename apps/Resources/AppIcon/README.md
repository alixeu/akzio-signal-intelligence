# Akzio Observatory App Icon

`Clarity Prism` 是当前选定方案：三个独立信息面围绕一个清晰焦点，表达“从多面复杂信息中形成单一判断”。构图不包含字母或字体轮廓。珊瑚色菱形只标记被确认的信号；Mono 模式即使移除色差，三面围合的轮廓仍保持识别。

## 已核实的项目边界

- SwiftPM 最低版本是 macOS 26.0（`apps/Package.swift` 与 `Info.plist.in` 一致）。
- 分发 App 不经过 Xcode project：`apps/Scripts/build_app.sh` 使用 `swift build`、`cargo build` 和手工 Bundle 布局。
- 当前打包链识别 `apps/Resources/AppIcon.icns`，并将它复制到 `Contents/Resources/AppIcon.icns`。`Info.plist.in` 通过 `CFBundleIconFile = AppIcon` 关联该兼容资源。
- 本机运行 macOS 27.0；实际安装并打开了 Apple Icon Composer 27.0（Bundle version 129，Bundle ID `com.apple.IconComposer`）。独立工具通过真实 UI 导入 SVG、配置材质并保存了 `AppIcon.icon`。
- Apple 当前官方页面说明独立 Icon Composer 要求 macOS 26.4 或更高；本机系统满足要求。官方工作流仍要求在 Icon Composer 中保存 `.icon`，并由 Xcode target 选择同名 App Icon。原始 `.icon` 已由工具验证有效，但当前纯 SwiftPM 手工打包脚本仍不能把它当作已编译 App Icon 直接替代 `AppIcon.icns`。

## 候选与选择

| 候选 | 小尺寸 | 独特性 | 分层实现 | 结论 |
|---|---:|---:|---:|---|
| 01 Convergent Facets | 4/5 | 4/5 | 5/5 | 汇聚明确，但横向动势较强 |
| 02 Signal Lens | 4/5 | 4/5 | 5/5 | 有观察语义，但轮廓更接近通用镜片 |
| 03 Clarity Prism | 5/5 | 5/5 | 5/5 | 选定；无字母骨架，三面围合在小尺寸仍成立 |

候选对照见 `previews/AppIcon-candidates.png`。所有候选均为原创几何，不包含字母或字体字形。

## 交付文件

- `sources/layers/01-primary-mark.svg`：后层，透明 1024 × 1024 画布，三个暖金信息面。
- `sources/layers/02-signal-accent.svg`：前层，透明 1024 × 1024 画布，珊瑚端点。
- `sources/candidates/*.svg`：三个设计候选，仅用于评审；其圆角背景不是 Icon Composer 导入层。
- `../AppIcon.icon`：由 Icon Composer 27.0 实际保存的原生多层文件，内部 `icon.json` 与 `Assets/` 由工具生成，不手写 Schema。
- `previews/AppIcon-flat.svg` / `.png`：1024 × 1024 高清扁平预览，也是 `.icns` 的确定性来源。
- `previews/AppIcon-appearances.svg` / `.png`：Default、Dark、Mono 的静态颜色参考；不是系统实时材质截图。
- `previews/sizes/{16,32,64,128,1024}.png`：小尺寸检查样本。
- `../AppIcon.icns`：当前自定义打包链使用的兼容 App Icon。

## Icon Composer 组装参数

`apps/Resources/AppIcon.icon` 已由 Icon Composer 27.0 保存并重新打开验证。不要手写、解包或仿造 `.icon` 内部结构。

1. Document 仅启用 iOS/macOS 中的 macOS；关闭本项目不存在的 watchOS 范围。
2. Canvas 使用 1024 × 1024。背景直接在 Icon Composer 配置，不导入背景 SVG，也不添加系统圆角蒙版。
3. 按文件名前缀导入两个透明 SVG，保持坐标、缩放、位移均为默认值：
   - 后层/主组：`01-primary-mark.svg`
   - 前层/强调组：`02-signal-accent.svg`
4. Default：背景从 `#23201C` 到 `#161616` 的克制对角渐变；主层 `#D4A15E`；强调层 `#FF6B4A`。
5. Dark：背景从 `#1A1816` 到 `#0E0E0E`；主层提亮为 `#E6B877`；强调层 `#FF7758`，确保主层不融入背景。
6. Mono：背景从 `#272727` 到 `#131313`；主层 `#F3EFE9`；强调层 `#9C9892`。识别依靠负空间和明度，不依赖金/橙色差。
7. 主组启用 Liquid Glass；从工具的自动/默认材质开始，只增加足以分离轮廓的折射、镜面高光和中性阴影。避免边缘发白，确保 Effects Off 时标识仍完整。
8. 强调组保持不透明；如果自动玻璃让菱形在 16–32 px 变成圆枕状，关闭该层 Liquid Glass 和镜面高光。不要在 SVG 中烘焙阴影、高光、模糊或折射。
9. 在 Mono 的 Options 中分别检查 Clear light、Clear dark、Tinted light、Tinted dark。Clear/Tinted 是由 Mono 注解产生的预览，不是第四套 Logo；在复杂背景图上确认主轮廓仍可见。
10. 本次已在 Icon Composer 中检查 Effects On/Off、Default/Dark/Mono，并保存到 `AppIcon.icon`；平台设置为 macOS Only、watchOS off。随后实际重新打开验证了工作区副本。使用 Xcode target 时，App Icon 名称应为 `AppIcon`；当前 SwiftPM 手工打包仍以 `.icns` 运行时兼容资源为准，同时把 `.icon` 源资源复制进 Bundle 供后续 Xcode 接入。

以上字段名称和工作流来自 Apple 当前公开界面/文档；没有记录未经工具验证的内部 Schema 或命令行参数。具体材质数值必须在实际 Icon Composer 版本中依据预览微调，不能由静态 SVG 或 `.icns` 冒充已验证结果。

## 验证记录

- 扁平基础形：已生成 SVG/PNG；未使用模糊、阴影、镜面高光或折射。
- 小尺寸：已生成 16、32、64、128、1024 px 样本。16 px 仍能辨认三面围合；珊瑚焦点不是识别必要条件。
- 兼容打包：通过 `AppIcon.icns` 与 `CFBundleIconFile` 接入；需在最终 Bundle 中继续核对文件、Info.plist、签名与系统显示。
- 构建链改动：`apps/Scripts/build_app.sh` 已增加对 `AppIcon.icon` 的资源复制；本次新 Bundle 的完整重建被工作区既有 Rust 错误阻断（`crates/akzio-store/src/store/workflow/commits.rs:53` 的 `super::super::run_control` unresolved import），因此没有把资源复制说成已进入一个成功构建的 v2 Bundle。Swift App 阶段已通过，脚本 `bash -n` 已通过。
- 原生 `.icon`：已由 Icon Composer 27.0 真实生成并重新打开；`icon.json` 明确记录 `system-dark`、refractivity、两组图层和 macOS-only 平台范围。
- Liquid Glass：已在 Icon Composer 预览中实测 Default/Dark/Mono、Effects On/Off、主层折射和 Mono 灰阶映射。Clear/Tinted 需要在 Mono 的 Options 中进一步选择变体；本次工具版本 UI 未暴露可操作的 Options 控件，因此不把 Clear/Tinted 标成已完成。
- Finder：已验证兼容 `.icns` 的实际显示。Dock、应用切换器和 Xcode 编译后的原生 `.icon`：尚未验证；本次没有启动 Akzio App/Core。

## 官方依据

- Apple Developer: <https://developer.apple.com/icon-composer/>
- Apple Developer Documentation: <https://developer.apple.com/documentation/xcode/creating-your-app-icon-using-icon-composer>
- WWDC: <https://developer.apple.com/videos/play/wwdc2025/361/>
- Human Interface Guidelines: <https://developer.apple.com/design/human-interface-guidelines/app-icons>
