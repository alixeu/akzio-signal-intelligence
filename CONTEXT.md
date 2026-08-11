# Akzio Signal Intelligence

Akzio is a local, Paper-only multi-agent research system whose durable state and authority are owned by Rust. This glossary names the learning and execution concepts shared across its domain boundaries.

## Language

**Canonical Run**:
A scheduler-owned Paper run whose sealed outcomes may influence durable learning state.
_Avoid_: production run, live run

**Noncanonical Run**:
A Debug, Replay, Shadow, or Paper Dry Run whose artifacts may support diagnostics or comparison but may never directly promote memory, contracts, or topology.
_Avoid_: test run when the exact purpose matters

**Outcome Schedule**:
An immutable commitment to evaluate one canonical Paper decision, whether it ended in a durable NoOrder verdict or a reconciled commitment, at specified future trading-session horizons.
_Avoid_: immediate evaluation, evaluation timer

**Sealed Paper Outcome**:
An immutable outcome derived from a canonical Paper decision with durable terminal execution lineage and complete governed market evidence for its horizon.
_Avoid_: result, caller-supplied metrics

**Policy Subject**:
The typed memory, contract version, or topology version whose learning lifecycle may change.
_Avoid_: subject string, policy key

**Policy Influence**:
An Active or Proven learning artifact that was actually included in the context of a later decision.
_Avoid_: memory hint, hidden prior

**Shadow Pair**:
An immutable comparison between a canonical parent Paper decision and one noncanonical candidate decision evaluated over the same outcome horizon.
_Avoid_: A/B test, timestamp pair

**Candidate Policy**:
An immutable proposed contract or research-topology version progressing through bounded canary states without gaining new data-source, tool, or execution authority.
_Avoid_: active policy, permission expansion

**Paper Commitment**:
The single scheduler-owned broker commitment permitted for one broker session after all Rust execution gates accept it.
_Avoid_: live order, model order
