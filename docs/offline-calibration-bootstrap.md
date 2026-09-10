# Offline historical calibration bootstrap

`DecisionPolicy::default()` remains fail-closed: it has no calibrated assets or
risk model and therefore cannot produce a non-zero target. The bootstrap path is
an explicit offline build, separate from a live Paper run:

```text
historical forecasts + their later realized returns
       + common four-asset daily close panel
       + explicit risk limits
              ↓
akzio calibration build
              ↓
provenance-bearing frozen policy artifact
              ↓
execution.decision_policy_path
```

The input is `OfflineCalibrationInput` JSON. Every forecast record must name a
source Run, the immutable Decision and DecisionContext references, the effective
`research.synthesizer` route, provider/model identity, and its contract hash.
It must be timestamped before its realized return and finish at or before the
training cutoff. The price panel must contain exactly one monotonic series for
each executable asset and enough common observations. A missing horizon,
duplicate asset series, future timestamp, non-positive close, or insufficient
sample count fails the build.

The Store-backed preparation entry point is read-only with respect to the Store:

```bash
akzio --config config/akzio.paper-research.local.toml calibration export \
  --store /path/to/canonical-paper-store \
  --risk-limits approved-risk-limits.json \
  --output historical-calibration.json \
  --min-samples 30
```

It scans canonical Paper `Decision`/sealed `Outcome` pairs, derives each
asset's realized T+1/T+3/T+5 return from the frozen outcome bar evidence, and
writes a sibling `historical-calibration.report.json`. Unmatured Outcomes,
missing bars, identity mismatches, conflicting price points, and incomplete
sample counts remain explicit report gaps; they are never filled from targets,
later summaries, or synthetic values. The command does not write Store state,
canonical learning, or a policy file.

The builder computes ten probability bins per asset and horizon from the
historical forecast/outcome pairs. It also computes annualized volatility, Beta
against QQQ, expected shortfall, one-day gap loss, and the common return
covariance matrix. These values are Rust-produced; the input only supplies the
policy limits and the historical observations. No current T0 prices or current
forecast are used for fitting.

```bash
akzio calibration build --input historical-calibration.json \
  --output frozen-decision-policy.json
akzio calibration validate --input frozen-decision-policy.json
akzio calibration inspect --input frozen-decision-policy.json
```

The generated document records policy and algorithm versions, provider/model
route identity, synthesizer contract hash, creation and training windows,
source Runs, sample count, input/output hashes, and a risk-model hash. A policy
is still only a Decision input: directional Evidence and Critique verification
remain independent eligibility checks, and a valid risk model does not make a
missing Claim eligible. The loader binds the policy to the configured
`research.synthesizer` model/version and refuses an incomplete provenance
envelope.
