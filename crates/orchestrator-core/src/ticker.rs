use regex::Regex;
use std::collections::BTreeSet;

pub fn normalize_ticker(value: &str) -> String {
    value.split_whitespace().collect::<String>().to_uppercase()
}

pub fn parse_tickers(raw: impl AsRef<str>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for item in raw.as_ref().split(',') {
        let ticker = normalize_ticker(item);
        if ticker.is_empty() || ticker == "__ALL__" {
            continue;
        }
        if seen.insert(ticker.clone()) {
            out.push(ticker);
        }
    }
    out
}

pub fn display_ticker(tickers: &[String]) -> String {
    tickers.join(",")
}

pub fn slug_ticker(value: &str) -> String {
    static RE_TEXT: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE_TEXT.get_or_init(|| Regex::new(r"[^A-Za-z0-9]+").expect("valid regex"));
    re.replace_all(value.trim(), "_")
        .trim_matches('_')
        .to_uppercase()
}

pub fn run_slug(tickers: &[String]) -> String {
    let display = display_ticker(tickers);
    let slug = slug_ticker(&display.replace(',', "_"));
    if slug.is_empty() {
        "UNKNOWN".to_string()
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tickers_dedupes_and_uppercases() {
        assert_eq!(
            parse_tickers(" qqq, VIX,qqq, __ALL__ , soxx "),
            vec!["QQQ", "VIX", "SOXX"]
        );
    }

    #[test]
    fn run_slug_uses_underscores() {
        assert_eq!(run_slug(&parse_tickers("QQQ,VIX,SOXX")), "QQQ_VIX_SOXX");
    }
}
