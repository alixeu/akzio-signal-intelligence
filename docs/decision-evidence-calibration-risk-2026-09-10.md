# Decision evidence, calibration, risk, and quote follow-up

This follow-up preserves `31ce9b678e7648ac` and all other historical Runs. It
does not open the production Store, write broker state, or rewrite a failed
Attempt.

The Decision path now has an explicit offline bootstrap producer. `akzio
calibration build` accepts historical forecast/realized-return pairs, a common
four-asset daily close panel, and explicit risk limits. Rust rejects future
forecast samples, insufficient horizon samples, duplicate or missing assets,
non-positive prices, and an unmeasurable risk panel. It computes forecast
probability bins plus annualized volatility, QQQ Beta, expected shortfall, gap
loss, and covariance. The output is a provenance-bearing policy document with
policy/algorithm versions, training window, source Runs, sample count, input and
output hashes, and a risk-model hash. `calibration validate` and `inspect` load
the same document. An absent or invalid policy remains fail-closed.

DecisionContext now carries optional per-asset eligibility diagnostics:
directional evidence, claim verification, calibration, risk, eligible, and
specific reasons such as `missing_evidence`, `unverified_claim`,
`missing_calibration`, `insufficient_calibration_samples`, `risk_unknown`,
`confidence_too_low`, `horizon_conflict`, and `no_directional_signal`.

The existing NewsWeb adapter remains the only search path. Startup capability
probe now makes a real, Rust-bounded native-web probe and registers NewsWeb only
when the provider response and citations pass the same policy used by the
adapter. The read-only CLI preflight is:

```bash
akzio evidence preflight \
  --resource 'news:QQQ:YYYY-MM-DD:YYYY-MM-DD:market'
```

It reports source, published/retrieved/usable times, asset/domain, citations,
quality, and raw/normalized hashes; it deliberately does not write a Store
artifact. On 2026-09-10 the configured gateway returned HTTP 502 with
`unknown provider model gpt-5.6-luna`, so NewsWeb remains BLOCKED. No unavailable
need was changed to available.

Execution refresh now distinguishes a received-but-invalid quote from a
missing quote. It retains valid account/clock reads, records a quote validation
error, and lets ExecutionGate emit `HardBlocker::InvalidQuote` while Broker
policy remains forbidden. The existing bid/ask safety rule is unchanged. The
regression suite covers delayed responses, future evidence, and non-positive
quotes.
