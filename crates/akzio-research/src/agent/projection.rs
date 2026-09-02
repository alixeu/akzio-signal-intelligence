#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoricalProjection {
    condition: akzio_domain::ExperimentCondition,
    cutoff: chrono::NaiveDate,
}

impl HistoricalProjection {
    const fn new(
        condition: akzio_domain::ExperimentCondition,
        cutoff: chrono::NaiveDate,
    ) -> Self {
        Self { condition, cutoff }
    }

    fn project_request(&self, request: &mut AgentModelRequest) {
        if !self.masks_identifiers() && !self.masks_calendar() {
            return;
        }

        request.prompt = self.project_text(&request.prompt);
        request.objective = self.project_text(&request.objective);
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

    fn restore_turn(&self, turn: &mut AgentModelTurn) {
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

    const fn masks_identifiers(&self) -> bool {
        matches!(
            self.condition,
            akzio_domain::ExperimentCondition::IdentifierMasked
                | akzio_domain::ExperimentCondition::FullyMasked
        )
    }

    const fn masks_calendar(&self) -> bool {
        matches!(
            self.condition,
            akzio_domain::ExperimentCondition::CalendarMasked
                | akzio_domain::ExperimentCondition::FullyMasked
        )
    }

    fn project_value(&self, value: &mut Value) {
        self.map_value(value, false);
    }

    fn restore_value(&self, value: &mut Value) {
        self.map_value(value, true);
    }

    fn map_value(&self, value: &mut Value, reverse: bool) {
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

fn map_identifier_tokens(text: &str, reverse: bool) -> String {
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

fn map_calendar_dates(text: &str, cutoff: chrono::NaiveDate, reverse: bool) -> String {
    use std::sync::OnceLock;

    static ISO_DATE: OnceLock<Regex> = OnceLock::new();
    static RELATIVE_DATE: OnceLock<Regex> = OnceLock::new();

    if reverse {
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

