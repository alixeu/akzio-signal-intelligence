# Retired workflow history fixture

This is an offline fixture run captured from revision `3c44f91caf791404f64645852f6fda1428550a8f`, with the release identity constants advanced to Contract 63 / PromptBundle 35 (candidate 64). It is **not** the audited real-model run. Run `13b88b7ce6fe4c45` completed the former Planner / PaperDryRun chain using deterministic adapters only.

The SQL contains immutable artifacts, compressed payloads, graph revisions, task snapshots and event records. It excludes schema, Store metadata, credentials and daemon tokens. All market/model content is synthetic. Test code imports these frozen rows into a new isolated Store solely for read/export/integrity and retirement checks; there is no production import or legacy execution switch.

Outcome Contract hash: `c9556a7ca9000cd06b96e385876013e3ce06a330fb4d01a473a43ef2db067ddf` (version 63, PromptBundle 35). This matches the unchanged Outcome Contract installed by the unified release.
