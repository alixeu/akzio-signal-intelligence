use super::*;
use crate::{ArtifactId, DecisionDraft};
use serde_json::json;

fn reference(kind: ArtifactKind, bytes: &[u8]) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(bytes)),
        kind,
    }
}
fn proposal() -> DecisionDraft {
    serde_json::from_value(json!({"summary":"neutral test","confidence_ppm":500000,
        "forecasts":Asset::EXECUTABLE.iter().flat_map(|a| ["t1","t3","t5"].iter().map(move |h|
            json!({"asset":a.symbol(),"horizon":h,"positive_return_probability_ppm":500000,"expected_return_ppm":0}))).collect::<Vec<_>>(),
        "numeric_basis":proposal_review_keys().iter().map(|scope|json!({"scope":scope,"inputs":[reference(ArtifactKind::NormalizedEvidence,b"evidence")],
            "units":"ppm","method":"neutral prior","assumptions":"symmetric","uncertainty":"uncalibrated"})).collect::<Vec<_>>(),
        "claims":[],"critiques":[],"evidence":[],"material_conflicts":[],"hard_blockers":[],"soft_warnings":[]})).unwrap()
}
fn review() -> ProposalReview {
    ProposalReview {
        proposal: reference(ArtifactKind::DecisionProposal, b"proposal"),
        proposal_hash: ContentHash::of_bytes(b"payload"),
        manifest: reference(ArtifactKind::ContextManifest, b"manifest"),
        contract_hash: ContentHash::of_bytes(b"contract"),
        assessments: proposal_review_keys()
            .into_iter()
            .map(|scope| ProposalAssessment {
                scope,
                accepted: true,
                rationale: "bounded neutral prior".into(),
                evidence_refs: vec![],
                issues: vec![],
            })
            .collect(),
    }
}
fn reject(review: &mut ProposalReview, scope: &str) {
    let row = review
        .assessments
        .iter_mut()
        .find(|a| a.scope == scope)
        .unwrap();
    row.accepted = false;
    row.issues = vec![ProposalIssue {
        category: ProposalIssueCategory::EstimateBasisMismatch,
        field_path: format!("{scope}.expected_return_ppm"),
        correction_criterion: "Match the stated estimate basis".into(),
        evidence_refs: vec![],
    }];
}
#[test]
fn issues_require_rejected_scope_and_matching_field() {
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    r.validate_for_contract(69).unwrap();
    let a = r.assessments.iter_mut().find(|a| !a.accepted).unwrap();
    a.issues[0].field_path = "forecast.QQQ.t3.expected_return_ppm".into();
    assert!(r.validate_for_contract(69).is_err());
    let mut r = review();
    r.assessments[0].accepted = false;
    assert!(r.validate_for_contract(69).is_err());
}
#[test]
fn legacy_review_keeps_decoding_without_issues() {
    let mut value = serde_json::to_value(review()).unwrap();
    value["assessments"][0]["accepted"] = json!(false);
    let old: ProposalReview = serde_json::from_value(value).unwrap();
    old.validate_for_contract(67).unwrap();
    assert!(old.validate_for_contract(69).is_err());
}
#[test]
fn issue_id_ignores_prose() {
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    let a = r.assessments.iter_mut().find(|a| !a.accepted).unwrap();
    let before = a.issues[0].stable_id(&a.scope);
    a.issues[0].correction_criterion = "Different prose, same issue".into();
    assert_eq!(before, a.issues[0].stable_id(&a.scope));
}
#[test]
fn revision_preserves_accepted_forecasts_and_basis() {
    let prior = proposal();
    let mut next = prior.clone();
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    next.forecasts[0].expected_return_ppm = 100;
    validate_proposal_revision(&prior, &next, &r).unwrap();
    next.forecasts[1].expected_return_ppm = 100;
    assert!(validate_proposal_revision(&prior, &next, &r).is_err());
    let mut next = prior.clone();
    next.numeric_basis
        .iter_mut()
        .find(|b| b.scope == "forecast.QQQ.t1")
        .unwrap()
        .method = "changed".into();
    assert!(validate_proposal_revision(&prior, &next, &r).is_err());
}
#[test]
fn allocation_revision_can_rebalance_without_freezing_portfolio_dependencies() {
    let prior = proposal();
    let mut next = prior.clone();
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    next.numeric_basis
        .iter_mut()
        .find(|b| b.scope == "allocation.cash")
        .unwrap()
        .method = "cash depends on repair".into();
    validate_proposal_revision(&prior, &next, &r).unwrap();
}
#[test]
fn stagnation_ignores_reworded_rationale() {
    let p = proposal();
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    let mut next = r.clone();
    next.assessments
        .iter_mut()
        .for_each(|a| a.rationale = "rephrased".into());
    assert!(proposal_revision_stagnated(&p, &r, &p, &next));
    let mut changed = p.clone();
    changed.forecasts[0].expected_return_ppm = 1;
    assert!(!proposal_revision_stagnated(&p, &r, &changed, &next));
}
#[test]
fn new_evidence_breaks_stagnation() {
    let p = proposal();
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    let mut next = r.clone();
    next.assessments
        .iter_mut()
        .find(|a| !a.accepted)
        .unwrap()
        .issues[0]
        .evidence_refs
        .push(reference(ArtifactKind::NormalizedEvidence, b"new"));
    assert!(!proposal_revision_stagnated(&p, &r, &p, &next));
}
#[test]
fn accepted_scope_progress_breaks_stagnation() {
    let p = proposal();
    let mut r = review();
    reject(&mut r, "forecast.TQQQ.t1");
    reject(&mut r, "forecast.QQQ.t3");
    let mut next = r.clone();
    let improved = next
        .assessments
        .iter_mut()
        .find(|a| a.scope == "forecast.QQQ.t3")
        .unwrap();
    improved.accepted = true;
    improved.issues.clear();
    assert!(!proposal_revision_stagnated(&p, &r, &p, &next));
}
