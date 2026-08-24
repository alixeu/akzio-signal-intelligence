# Rubric Scoring for LLM and Agent Evaluation

**Research date:** 2026-08-21  
**Scope:** First-party documentation and primary academic papers only.

## Short Definition

A **rubric** is the evaluation specification used to judge an output. It defines:

- the dimension or criterion being assessed;
- what observable behavior counts as evidence;
- the allowed score or verdict scale;
- score anchors, examples, or descriptions for each level; and, optionally,
- aggregation and acceptance rules.

The rubric is not the score. A human or judge model applies the rubric and produces a score or verdict. Prometheus 2 gives the most explicit formulation: a score rubric contains a criterion description plus descriptions for each score in the range.[^prometheus]

## How Rubrics Are Structured

### Dimensions and criteria

Dimensions are the qualities being measured, such as correctness, relevance, factual grounding, safety, instruction following, or tool-use quality. A criterion should be specific enough that two judges can apply it consistently. G-Eval examples use coherence, consistency, fluency, and relevance; its prompt also adds evaluation steps that operationalize the criterion.[^geval]

For agents, Google documents criteria such as final-answer quality, instruction following, tool selection and parameter correctness, task success, and trajectory quality. Its trajectory rubric explicitly covers causal validity, efficiency, and adaptive robustness.[^google_rubrics]

Rubrics may be **static** (the same criteria are used for every item) or **adaptive** (criteria are generated for each prompt or task and then validated).[^google_rubrics]

### Numeric scales

There is no universal rubric scale:

- OpenAI string checks are binary `0` or `1`; similarity graders produce a metric score; score-model graders accept a configured numeric range and default nonnumeric output to `0`.[^openai_graders]
- G-Eval uses anchored 1-5 and 1-3 examples, such as coherence from lowest to highest quality and engagingness from dull to interesting.[^geval]
- Prometheus 2 uses 1-5 for direct assessment and defines a rubric with descriptions for each score.[^prometheus]
- Google adaptive rubrics return per-rubric verdicts and define the aggregate as the response's passing rate; some static metrics also return a 0-1 ratio.[^google_rubrics]

The important property is not the number of levels but the anchors: what makes a response a 1 rather than a 2, or a 4 rather than a 5.

## Aggregation and Thresholds

**Weighted aggregation is optional, not inherent to the word rubric.** OpenAI's `multi` grader combines subgrader outputs with an explicit formula, for example `0.5 * criterion_a + 0.5 * criterion_b`. This supports weighted dimensions and mixed binary/continuous subgrades.[^openai_graders] G-Eval uses a different weighting idea: it computes a probability-weighted sum over the possible rating tokens to turn a discrete 1-5 judgment into a smoother score.[^geval]

A **pass/fail threshold** is a separate policy decision. OpenAI exposes `pass_threshold` for applicable graders such as text similarity and score-model graders. A binary string check has no need for a separate threshold. A multigrader can produce a continuous aggregate, after which the caller can define an acceptance threshold.[^openai_graders] Google's documented rubric score is a passing rate, but the cited metric specification does not prescribe one universal system-level cutoff.[^google_rubrics]

For high-consequence evaluation, a weighted mean should not silently erase a critical failure. Keep hard requirements as explicit gates or separate criteria; use aggregation for tradeoffs among noncritical dimensions.

## Judge Reliability and Calibration

Reliability is evidence that the judge tracks a trusted target and behaves consistently. Useful checks include:

- agreement or correlation with expert/human labels on a held-out calibration set;
- repeated judgments of the same item, including judge-model multi-sampling;
- perturbation tests, such as swapping answer positions or removing irrelevant verbosity;
- adversarial cases designed to expose grader hacking or shortcut rewards; and
- separate validation and test sets when tuning prompts, weights, or thresholds.

OpenAI recommends comparing model-grader ordering against trusted human grades, using many good and bad examples, and checking for grader hacking against expert evaluations.[^openai_graders] Google recommends response flipping to reduce pairwise position bias and multi-sampling to improve consistency; its documented sampling count is 1-32, with a default of 4 as a latency/consistency tradeoff.[^google_judge]

The primary judge studies show why calibration is necessary. G-Eval reports meaningful human correlation but also warns about evaluator bias toward LLM-generated text.[^geval] The MT-Bench study reports over 80% agreement for strong judges in its setting, while finding position, verbosity, and possible self-enhancement biases. It also states that higher consistency from few-shot prompting does not necessarily imply higher accuracy.[^mtbench]

Thus, a stable judge is not automatically a calibrated judge. Calibrate the rubric and judge against trusted labels, then tune thresholds on validation data rather than selecting them from the judge's self-report.

## Does Model Self-Confidence Count as a Rubric Score?

**No, not by default.** A model's `confidence` is a claim about its own certainty. A rubric score is an assessment of the output against externally specified criteria. They can be stored together, but they answer different questions.

Self-confidence can become a rubric dimension only when the rubric explicitly evaluates confidence calibration, for example by comparing reported confidence with correctness over many labeled cases. One confidence value on one answer is not evidence that the answer is correct.

G-Eval's use of output-token probabilities is also not model self-confidence. Those probabilities are used by the evaluator to smooth the rating distribution and compute a score; they are not a self-assessed quality field.[^geval]

## Comparison with the Current Repository

The current tree contains evaluation-adjacent fields, but I did not find an explicit rubric model with criterion definitions, score anchors, judge identity/reliability, and rubric aggregation:

- `DeliberationSummary.confidence_ppm`, alternative-match weights, and uncertainty weights are model-assessed metadata in `crates/akzio-domain/src/contract.rs:97-171` and `crates/akzio-research/src/agent_v2.rs:333`; they are not rubric scores.
- `OutcomeWindow` contains measurable outcome dimensions such as utility, optional calibration, evidence completeness, and risk recall in `crates/akzio-domain/src/evaluation.rs:130-158`.
- `EvaluationPolicy` contains explicit minimum evidence-completeness and risk-recall thresholds in `crates/akzio-learning/src/evaluation.rs:30-54`; these are policy gates, not rubric anchors.
- `FrozenEvidenceMetrics` contains deterministic schema, evidence, blocker-recall, cost, and latency metrics in `crates/akzio-learning/src/frozen_eval.rs:63-103`.
- `Evaluation` records marginal utility, token cost, and latency in `crates/akzio-domain/src/evaluation.rs:690-711`.

In short: the repository currently has deterministic metrics, provenance confidence, and threshold gates. A rubric comparison would need to keep those concepts separate from a declared judge-applied scoring specification.

## Sources

- [OpenAI, Graders](https://developers.openai.com/api/docs/guides/graders) - grader types, numeric ranges, pass thresholds, multigrader formulas, judge validation, and grader-hacking checks. Accessed 2026-08-21.
- [OpenAI, Working with evals](https://developers.openai.com/api/docs/guides/evals) - eval datasets, testing criteria, and per-criterion pass/fail results. Accessed 2026-08-21.
- [Google Cloud, Details for managed rubric-based metrics](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/rubric-metric-details) - static versus adaptive rubrics, agent dimensions, verdicts, and passing-rate scores. Accessed 2026-08-21.
- [Google Cloud, Configure a judge model](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/configure-judge-model) - response flipping, multi-sampling, and judge-model configuration. Accessed 2026-08-21.
- [Liu et al., G-Eval: NLG Evaluation using Gpt-4 with Better Human Alignment](https://arxiv.org/html/2303.16634) - primary paper on criteria, anchored scales, probability-weighted scoring, human correlation, and evaluator bias. Accessed 2026-08-21.
- [Zheng et al., Judging LLM-as-a-Judge with MT-Bench and Chatbot Arena](https://arxiv.org/html/2306.05685v4) - primary study of judge agreement, bias, and consistency mitigations. Accessed 2026-08-21.
- [Kim et al., Prometheus 2: An Open Source Language Model Specialized in Evaluating Other Language Models](https://arxiv.org/html/2405.01535) - primary definition of score rubrics and direct-assessment scales. Accessed 2026-08-21.

[^openai_graders]: [OpenAI, Graders](https://developers.openai.com/api/docs/guides/graders).
[^google_rubrics]: [Google Cloud, Details for managed rubric-based metrics](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/rubric-metric-details).
[^google_judge]: [Google Cloud, Configure a judge model](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/configure-judge-model).
[^geval]: [Liu et al., G-Eval](https://arxiv.org/html/2303.16634).
[^mtbench]: [Zheng et al., MT-Bench and Chatbot Arena](https://arxiv.org/html/2306.05685v4).
[^prometheus]: [Kim et al., Prometheus 2](https://arxiv.org/html/2405.01535).
