#[test]
fn paper_money_parser_rejects_precision_loss_and_missing_whole_digits() {
    assert!(crate::parse_money_micros(&serde_json::json!("1.0000001")).is_none());
    assert!(crate::parse_money_micros(&serde_json::json!(".5")).is_none());
    assert_eq!(
        crate::parse_money_micros(&serde_json::json!("0.500000")),
        Some(akzio_domain::MoneyMicros(500_000))
    );
}

#[test]
fn alpaca_paper_transport_is_endpoint_and_resource_fenced() {
    for endpoint in [
        "https://api.alpaca.markets",
        "http://paper-api.alpaca.markets",
        "https://paper-api.alpaca.markets.evil.test",
        "https://paper-api.alpaca.markets/v2",
        "https://paper-api.alpaca.markets?live=true",
    ] {
        assert!(AlpacaPaperEvidenceTransport::new(endpoint, "key", "secret", None).is_err());
    }
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1&adjustment=all"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:6").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=6&adjustment=all&start=2026-08-01"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:1d:2026-08-20").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=5Min&limit=1000&adjustment=all&start=2026-08-20"
    );
    assert_eq!(
        AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:3m:2026-05-12").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1000&adjustment=all&start=2026-05-12"
    );
    assert!(AlpacaPaperEvidenceTransport::path_for("observer.qqq_history:all:2026-05-12").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:1d:2026-08-01:253").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:SPY:1d").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("bars:QQQ:5m").is_err());
    assert!(AlpacaPaperEvidenceTransport::path_for("https://example.com").is_err());

    let transport = AlpacaPaperEvidenceTransport::new(
        "https://paper-api.alpaca.markets",
        "key",
        "secret",
        Some(AlpacaMarketDataFeed::Iex),
    )
    .unwrap();
    assert_eq!(
        transport.base_url_for("paper.account"),
        "https://paper-api.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("paper.clock"),
        "https://paper-api.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("paper.quotes"),
        "https://data.alpaca.markets"
    );
    assert_eq!(
        transport.base_url_for("bars:QQQ:1d"),
        "https://data.alpaca.markets"
    );
    assert_eq!(
        transport.configured_path_for("paper.quotes").unwrap(),
        "/v2/stocks/quotes/latest?symbols=TQQQ,QQQ,SOXX,SOXL&feed=iex"
    );
    assert_eq!(
        transport.configured_path_for("bars:QQQ:1d").unwrap(),
        "/v2/stocks/QQQ/bars?timeframe=1Day&limit=1&adjustment=all&feed=iex"
    );
}
