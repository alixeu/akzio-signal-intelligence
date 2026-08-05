//! Private Markdown-to-HTML rendering helpers for human-readable reports.

use html_escape::encode_text;
use pulldown_cmark::{html, Options, Parser};

/// CSS injected into the generated HTML report.
const REPORT_CSS: &str = r#"
body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; color: #1a1a1a; line-height: 1.5; }
h1 { border-bottom: 2px solid #e0e0e0; padding-bottom: 8px; }
h2 { color: #333; margin-top: 28px; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; }
th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
th { background-color: #f5f5f5; font-weight: 600; }
hr { border: none; border-top: 1px solid #e0e0e0; margin: 24px 0; }
strong { color: #1a1a1a; }
ul, ol { padding-left: 24px; }
"#;

/// Convert a Markdown report into a standalone, styled HTML document.
///
/// Uses `pulldown-cmark` for spec-compliant CommonMark parsing and wraps the
/// fragment in a full `<!doctype html>` document with inline CSS so it renders
/// correctly in email clients.
pub fn report_to_html(markdown: &str) -> String {
    // Enable table and strikethrough extensions so the Trade Plan and Portfolio
    // Allocation Markdown tables render as proper <table> elements.
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(markdown, options);
    let mut body = String::new();
    html::push_html(&mut body, parser);

    let title = markdown
        .lines()
        .find_map(|line| line.strip_prefix("# ").map(str::trim))
        .unwrap_or("Strategy Report");

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>{title}</title>\
        <style>{css}</style></head><body>{body}</body></html>",
        title = encode_text(title),
        css = REPORT_CSS,
        body = body
    )
}
