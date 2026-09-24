// 历史实验投影在请求进入模型前做标识符/日历遮蔽，返回后尝试还原到
// Rust/Store 使用的真实值。遮蔽改变的是模型可见文本，不改变 Manifest、Evidence
// 或 Contract 身份。日期逆向仅匹配固定宽度的 DAY token；超范围不能宣称
// 无损还原，实验映射也不是业务事实或授权。
// 文件导读：`agent.rs` 通过 `include!` 把本文件的代码并入 AgentRuntime 所在模块，
// 因而这里直接复用父模块的请求、turn、JSON、Regex 与错误类型，并非独立子模块。
// Daemon 仅在配置历史评估条件和知识截止日期时调用 `with_historical_projection`；
// `runtime_run.rs` 在模型调用前投影请求、收到 turn 后恢复模型可写内容。建议按
// `new` → `project_request` / `map_value` → `restore_turn` 阅读；重点是可变借用、
// JSON 递归、`mem::take` 的所有权转移，以及 `OnceLock` 对固定正则的延迟初始化。

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoricalProjection {
    condition: akzio_domain::ExperimentCondition,
    cutoff: chrono::NaiveDate,
}

impl HistoricalProjection {
    // 把实验条件与日期截止点按值保存；两字段实现 Copy，所以构造器不借用调用方数据。
    // `const fn` 也允许常量上下文调用，但普通运行时构造同样有效。
    const fn new(
        condition: akzio_domain::ExperimentCondition,
        cutoff: chrono::NaiveDate,
    ) -> Self {
        Self { condition, cutoff }
    }

    // 原地改写调用方仍持有的请求，仅在对应实验条件开启时遮蔽模型可见文本；
    // `&mut` 借用在本次调用结束后归还，后续仍用同一个 AgentModelRequest 发起请求。
    fn project_request(&self, request: &mut AgentModelRequest) {
        // Request 内既有 prose，也有 JSON context、ToolResult、Schema 和 terminal
        // 定义；逐个位置投影可避免模型从 key、错误反馈或 continuation 泄露真实值。
        if !self.masks_identifiers() && !self.masks_calendar() {
            return;
        }

        request.prompt = self.project_text(&request.prompt);
        request.objective = self.project_text(&request.objective);
        // `iter_mut` 逐项产生可变借用，`for_each` 会立即消费迭代器；这不是延迟执行的处理链。
        request
            .context
            .iter_mut()
            .for_each(|value| self.project_value(value));
        request
            .tool_outputs
            .iter_mut()
            .for_each(|output| self.project_value(&mut output.output));
        if let Some(instruction) = request.continuation_instruction.as_mut() {
            *instruction = self.project_text(instruction);
        }
        for tool in &mut request.tools {
            tool.description = self.project_text(&tool.description);
            self.project_value(&mut tool.input_schema);
        }
        if let Some(terminal) = request.terminal.as_mut() {
            terminal.description = self.project_text(&terminal.description);
            self.project_value(&mut terminal.input_schema);
        }
    }

    // 原地还原模型返回中可由模型写入的文本/参数；provider continuation 与 usage 属于
    // 调用身份和遥测，不经过字符串替换，避免把模型输出误当作可改写的审计来源。
    fn restore_turn(&self, turn: &mut AgentModelTurn) {
        // 只还原模型可写字段；continuation identity 与 telemetry 不在映射范围内，
        // 这样恢复时仍由原始 provider/Store 记录判断，不篡改调用证据。
        if !self.masks_identifiers() && !self.masks_calendar() {
            return;
        }

        if let Some(text) = turn.assistant_text.as_mut() {
            *text = self.restore_text(text);
        }
        for call in &mut turn.tool_calls {
            self.restore_value(&mut call.arguments);
        }
        if let Some(submission) = turn.terminal_submission.as_mut() {
            self.restore_value(&mut submission.arguments);
        }
    }

    // `matches!` 将两个需要隐藏资产标识的实验枚举分支合并成布尔判断；
    // 其他条件返回 false，因此后续投影会保留资产 token。
    const fn masks_identifiers(&self) -> bool {
        matches!(
            self.condition,
            akzio_domain::ExperimentCondition::IdentifierMasked
                | akzio_domain::ExperimentCondition::FullyMasked
        )
    }

    // 仅 CalendarMasked 与 FullyMasked 改写日期；这个结果与资产标识遮蔽彼此独立。
    const fn masks_calendar(&self) -> bool {
        matches!(
            self.condition,
            akzio_domain::ExperimentCondition::CalendarMasked
                | akzio_domain::ExperimentCondition::FullyMasked
        )
    }

    // 正向包装器：递归修改可变 JSON 值，不克隆整棵树，也不改变 Null/布尔/数字。
    fn project_value(&self, value: &mut Value) {
        self.map_value(value, false);
    }

    // 反向包装器：使用同一递归器按相反方向还原字符串与对象键。
    fn restore_value(&self, value: &mut Value) {
        self.map_value(value, true);
    }

    // 递归遍历 JSON：数组元素借用后逐项改写；对象则先移出 Map，再移动每个 key/value
    // 递归处理并插回，避免一边遍历原 Map 一边修改它。标量分支没有需遮蔽的文本。
    fn map_value(&self, value: &mut Value, reverse: bool) {
        // Object key 也必须映射。std::mem::take 暂时移出 Map，避免在迭代期间修改
        // 容器；递归处理数组/对象，标量 null/bool/number 不含待遮蔽文本。
        match value {
            Value::String(text) => {
                *text = if reverse {
                    self.restore_text(text)
                } else {
                    self.project_text(text)
                };
            }
            Value::Array(values) => {
                for value in values {
                    self.map_value(value, reverse);
                }
            }
            Value::Object(values) => {
                let original = std::mem::take(values);
                for (key, mut value) in original {
                    self.map_value(&mut value, reverse);
                    let mapped_key = if reverse {
                        self.restore_text(&key)
                    } else {
                        self.project_text(&key)
                    };
                    values.insert(mapped_key, value);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    // 创建新的拥有型 String：先按资产 token 规则映射，再按日期规则映射；
    // 两个阶段的顺序与 restore_text 相反，便于把组合投影逆序还原。
    fn project_text(&self, text: &str) -> String {
        let text = if self.masks_identifiers() {
            map_identifier_tokens(text, false)
        } else {
            text.to_owned()
        };
        if self.masks_calendar() {
            map_calendar_dates(&text, self.cutoff, false)
        } else {
            text
        }
    }

    // 逆向恢复日期后再恢复资产名。输入只借用原文，返回值拥有新字符串，
    // 所以 turn 中原字段可在调用结束时由返回值替换。
    fn restore_text(&self, text: &str) -> String {
        let text = if self.masks_calendar() {
            map_calendar_dates(text, self.cutoff, true)
        } else {
            text.to_owned()
        };
        if self.masks_identifiers() {
            map_identifier_tokens(&text, true)
        } else {
            text
        }
    }
}

// 仅替换独立的 ASCII 标识符 token，返回新 String；非标识字符作为分隔符原样复制，
// 因此不会把 `TQQQ` 这类完整代码拆成逐字符替换。
fn map_identifier_tokens(text: &str, reverse: bool) -> String {
    // 成功映射返回程序内字面量的 `&'static str`；这个静态生命周期约束的是替换文本，
    // 而不是输入 token。None 表示未知 token，调用方继续写回原 token。
    fn mapped(token: &str, reverse: bool) -> Option<&'static str> {
        match (reverse, token) {
            (false, "TQQQ") => Some("ASSET_A"),
            (false, "QQQ") => Some("ASSET_B"),
            (false, "SOXX") => Some("ASSET_C"),
            (false, "SOXL") => Some("ASSET_D"),
            (true, "ASSET_A") => Some("TQQQ"),
            (true, "ASSET_B") => Some("QQQ"),
            (true, "ASSET_C") => Some("SOXX"),
            (true, "ASSET_D") => Some("SOXL"),
            _ => None,
        }
    }

    let mut output = String::with_capacity(text.len());
    let mut token = String::new();
    // 闭包把一个完整 token 原子写回，保证 SOXL 等标识符不会被逐字符替换成混合值。
    // 它只读取捕获的 reverse；output/token 由调用点以可变借用传入，未知值时
    // `Option::unwrap_or` 选原 token。flush 后清空临时 String，供下一个 token 复用。
    let flush = |output: &mut String, token: &mut String| {
        if token.is_empty() {
            return;
        }
        output.push_str(mapped(token, reverse).unwrap_or(token));
        token.clear();
    };

    for character in text.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            token.push(character);
        } else {
            flush(&mut output, &mut token);
            output.push(character);
        }
    }
    flush(&mut output, &mut token);
    output
}

// 在日期 token 与 cutoff 的相对 DAY 偏移之间转换。Regex 是固定代码常量而非外部输入；
// `OnceLock` 延迟编译并在后续调用复用，若硬编码模式写错，`unwrap` 会在首次初始化时 panic。
fn map_calendar_dates(text: &str, cutoff: chrono::NaiveDate, reverse: bool) -> String {
    // 正向格式化相对 cutoff 的 DAY 偏移，反向正则仅识别符号加六位数字；
    // 对更大偏移不保证往返一致。checked_add_signed 失败时保留所匹配 token。
    use std::sync::OnceLock;

    static ISO_DATE: OnceLock<Regex> = OnceLock::new();
    static RELATIVE_DATE: OnceLock<Regex> = OnceLock::new();

    if reverse {
        // `get_or_init` 只在首次需要时执行构造闭包；`replace_all` 对每个匹配调用回调，
        // 解析或日期加法失败时回写原 token，最终 `into_owned` 得到独立 String。
        let pattern = RELATIVE_DATE
            .get_or_init(|| Regex::new(r"DAY(?P<sign>[+-])(?P<days>[0-9]{6})").unwrap());
        pattern
            .replace_all(text, |captures: &regex::Captures<'_>| {
                let magnitude = captures["days"].parse::<i64>().unwrap_or_default();
                let days = if &captures["sign"] == "-" {
                    -magnitude
                } else {
                    magnitude
                };
                cutoff
                    .checked_add_signed(chrono::Duration::days(days))
                    .map_or_else(
                        || captures[0].to_owned(),
                        |date| date.format("%Y-%m-%d").to_string(),
                    )
            })
            .into_owned()
    } else {
        // 前向回调把合法 ISO 日期换成相对天数；日期文本无法解析时使用 captures[0] 原文。
        let pattern = ISO_DATE
            .get_or_init(|| Regex::new(r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})").unwrap());
        pattern
            .replace_all(text, |captures: &regex::Captures<'_>| {
                chrono::NaiveDate::parse_from_str(&captures["date"], "%Y-%m-%d").map_or_else(
                    |_| captures[0].to_owned(),
                    |date| format!("DAY{:+07}", (date - cutoff).num_days()),
                )
            })
            .into_owned()
    }
}
