// 递归收集 JSON 中所有字符串，供指令样内容等安全指标做完整遍历；函数只写入
// 调用方提供的输出 Vec，不改变输入 Value，也不承担 Artifact 授权判断。
fn collect_strings(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => output.push(value.clone()),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_strings(value, output);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_strings(value, output);
            }
        }
        _ => {}
    }
}
