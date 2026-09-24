"""Submit one formal PositionPlan or Paper Run to an already-running Akzio Core."""
# 文件职责：连接原配置指定的正式 Core，发布一次 Run，读取持久投影并归档脱敏报告。
# Python 不创建 Store/Policy、启动 Core、计算 Gate、批准交易或推进 Outcome。
import argparse
import datetime as dt
import hashlib
from http.client import IncompleteRead
import ipaddress
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import tomllib
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import zipfile


TERMINAL = {"completed", "completed_with_execution_rejection", "failed", "cancelled"}
SUCCESS = {"completed", "completed_with_execution_rejection"}


class RunError(Exception):
    """A rejected or unavailable control operation, not a business verdict."""


class AmbiguousSubmission(RunError):
    """The POST may have committed; never replay it automatically."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # The token belongs only to the configured loopback endpoint.
        return None


_opener = build_opener(ProxyHandler({}), _NoRedirect())


def _connection(config_path):
    source = Path(config_path).expanduser().resolve() if config_path else (
        Path.home() / ".akzio/config.toml")
    config = tomllib.loads(source.read_text())
    daemon = config["daemon"]
    if daemon.get("debug_control", False):
        raise RunError("配置启用了 debug_control；请连接正式 Core，不要使用隔离 Debug Store")
    address = daemon["http_addr"]
    url = urlsplit(f"http://{address}/")
    try:
        host = ipaddress.ip_address(url.hostname or "")
        port = url.port
    except ValueError as error:
        raise RunError("daemon.http_addr 必须是带端口的回环 IP 地址") from error
    if (not host.is_loopback or port is None or url.username or url.password
            or url.path != "/" or url.query or url.fragment):
        raise RunError("daemon.http_addr 必须是带端口的回环 IP 地址")
    store = Path(daemon["store_root"]).expanduser()
    if not store.is_absolute():
        # Rust interprets relative paths from its own working directory; a
        # standalone client cannot safely guess which Store its Core opened.
        raise RunError("请将 daemon.store_root 配置为绝对路径，以便确认正式 Core 身份")
    override = os.environ.get("AKZIO_STORE_ROOT")
    if override and Path(override).expanduser().resolve() != store.resolve():
        raise RunError("AKZIO_STORE_ROOT 与配置的 Store 不一致；拒绝猜测正式 Core 身份")
    try:
        token = (store / ".daemon-token").read_text()
    except FileNotFoundError as error:
        raise RunError("未找到正式 Core token；请先从 App 或 daemon serve 启动正式 Core") from error
    if not token.strip() or "\r" in token or "\n" in token:
        raise RunError("正式 Core token 无效")
    return CoreClient(f"http://{address}", token), config


class CoreClient:
    """Authenticated native loopback client; Rust remains the workflow authority."""

    def __init__(self, endpoint, token):
        self.endpoint = endpoint
        self.token = token

    def json(self, method, path, payload=None, *, timeout=30):
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(
            self.endpoint + path,
            data=body,
            method=method,
            headers={"x-akzio-token": self.token,
                     **({"content-type": "application/json"} if body is not None else {})},
        )
        try:
            with _opener.open(request, timeout=timeout) as response:
                return json.load(response)
        except HTTPError as error:
            if 300 <= error.code < 400:
                raise RunError("正式 Core 返回重定向；认证请求不会跟随 Location") from error
            if error.code == 401:
                raise RunError("正式 Core 认证失败；请检查配置、Store 和运行中的 Core") from error
            detail = ""
            try:
                value = json.loads(error.read(4096))
                detail = str(value.get("error", ""))[:300] if isinstance(value, dict) else ""
            except (ValueError, UnicodeError, OSError):
                pass
            message = f"正式 Core 拒绝 {method} {path}: HTTP {error.code}"
            if detail:
                message += f" ({detail})"
            if method == "POST" and path == "/runs" and error.code >= 500:
                raise AmbiguousSubmission(message + "；可能已创建 Run，请先查询正式 Store") from error
            raise RunError(message) from error
        except (ValueError, IncompleteRead) as error:
            if method == "POST" and path == "/runs":
                raise AmbiguousSubmission(
                    "Core 提交响应不完整；可能已创建 Run。请先检查正式 Store，勿直接重试"
                ) from error
            raise RunError("Core 返回无效的 JSON 投影；未改变既有 Run") from error
        except (URLError, TimeoutError, ConnectionError, OSError) as error:
            if method == "POST" and path == "/runs":
                raise AmbiguousSubmission(
                    "提交请求结果不确定；可能已创建 Run。请先检查正式 Store，勿直接重试"
                ) from error
            raise RunError(
                "无法连接正式 Core；请先从 App 或 daemon serve 启动，并检查配置端口"
            ) from error


def _artifact(artifacts, kind):
    # Observer is bounded. An absent artifact is unknown, not proof of absence.
    return next((item for item in reversed(artifacts) if item.get("kind") == kind), None)


def _summary(run_id, purpose, replay, detail):
    artifacts = detail.get("artifacts", []) if isinstance(detail, dict) else []
    decision = _artifact(artifacts, "decision")
    verdict = _artifact(artifacts, "execution_verdict")
    commitment = _artifact(artifacts, "execution_commitment")
    reconciliation = _artifact(artifacts, "reconciliation")
    receipts = [item for item in artifacts if item.get("kind") == "order_receipt"]
    schedule = _artifact(artifacts, "outcome_schedule")
    no_order = ((verdict or {}).get("payload") or {}).get("verdict") == "no_order"
    lifecycle = replay.get("lifecycle") or {}
    proposal = (decision or {}).get("payload", {})
    research_plan = proposal.get("research_plan") or {}
    validated = research_plan.get("validated") or {}
    research_progress = ((detail.get("research_audit") or {}).get("progress") or {}
                         if isinstance(detail, dict) else {})
    return {
        "run_id": run_id, "purpose": purpose,
        "status": replay.get("status", "unknown"),
        "lifecycle": {
            "outcome_scheduled": lifecycle.get("outcome_scheduled"),
            "numeric_outcome_sealed": lifecycle.get("numeric_outcome_sealed"),
            "retrospective_status": lifecycle.get("retrospective_status"),
        },
        "research": {
            "review_status": research_progress.get("review_status"),
            "proposal": research_progress.get("proposal"),
        },
        "decision": {
            "status": "observed" if decision else "not_observed_in_bounded_projection",
            "artifact_id": (decision or {}).get("artifact_id"),
            "targets": proposal.get("targets"),
            "research_plan": {
                "status": research_plan.get("status"),
                "execution_status": research_plan.get("execution_status"),
                "validated": {
                    "cash_weight_ppm": validated.get("cash_weight_ppm"),
                    "allocations": [
                        {"asset": allocation.get("asset"),
                         "target_weight_ppm": allocation.get("target_weight_ppm")}
                        for allocation in validated.get("allocations", [])
                    ],
                } if validated else None,
            } if research_plan else None,
        },
        "execution_verdict": (
            {"status": (verdict.get("payload") or {}).get("verdict"),
             "artifact_id": verdict.get("artifact_id"),
             "blockers": (verdict.get("payload") or {}).get("no_order", {}).get("blockers")}
            if verdict else {"status": "not_applicable" if purpose == "position_plan"
                             else "not_observed_in_bounded_projection"}
        ),
        "paper_commitment": (
            {"status": "persisted", "artifact_id": commitment.get("artifact_id")}
            if commitment else {"status": "not_applicable" if purpose == "position_plan"
                                else "no_order" if no_order
                                else "not_observed_in_bounded_projection"}
        ),
        "order_submission": (
            "not_applicable" if purpose == "position_plan"
            else "no_order" if no_order
            else "receipt_observed" if receipts else "not_observed"
        ),
        "paper_fills": (
            "not_applicable" if purpose == "position_plan"
            else "no_order" if no_order
            else [{"artifact_id": item.get("artifact_id"),
                   "state": (item.get("payload") or {}).get("state"),
                   "filled_quantity_micros": (item.get("payload") or {}).get("filled_quantity_micros")}
                  for item in receipts] if receipts else "not_observed"
        ),
        "reconciliation": (
            {"status": (reconciliation.get("payload") or {}).get("state"),
             "artifact_id": reconciliation.get("artifact_id")}
            if reconciliation else {"status": "not_applicable" if purpose == "position_plan"
                                    else "not_observed_in_bounded_projection"}
        ),
        "outcome_schedule": (
            {"status": "observed", "artifact_id": schedule.get("artifact_id")}
            if schedule else {"status": "not_applicable" if purpose == "position_plan"
                             else "not_observed_in_bounded_projection"}
        ),
    }


def _secrets(config, token):
    secrets = {token}

    def collect(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, str) and any(word in key.lower() for word in
                                                 ("key", "secret", "token", "password")):
                    secrets.update((item, os.path.expandvars(item)))
                else:
                    collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    collect(config)
    for name, value in os.environ.items():
        if any(word in name for word in ("API_KEY", "API_SECRET", "TOKEN", "PASSWORD")):
            secrets.add(value)
    secrets.discard("")
    return sorted(secrets | {json.dumps(value)[1:-1] for value in secrets}, key=len, reverse=True)


def _redact(text, secrets):
    for secret in secrets:
        text = text.replace(secret, "[REDACTED]")
    return text


def _archive(root, secrets):
    files = []
    for path in sorted(root.iterdir()):
        if path.is_symlink():
            raise RunError("报告包含符号链接；拒绝归档")
        if path.is_file() and path.suffix == ".json":
            files.append(path)
    bundle = root / "bundle"
    bundle_files = []
    if bundle.is_symlink():
        raise RunError("bundle 目录是符号链接；拒绝归档")
    if bundle.is_dir():
        bundle_files = [path for path in sorted(bundle.rglob("*")) if path.is_file()]
        if any(path.is_symlink() for path in bundle.rglob("*")):
            raise RunError("导出包含符号链接；拒绝归档")
        checksums = bundle / "checksums.sha256"
        if not checksums.is_file():
            raise RunError("Run bundle 缺少 checksums.sha256；保留目录")
        expected = {}
        for line in checksums.read_text().splitlines():
            digest, separator, relative = line.partition("  ")
            relative_path = Path(relative)
            if (not separator or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)
                    or relative_path.is_absolute() or ".." in relative_path.parts
                    or relative in expected):
                raise RunError("Run bundle 校验清单无效；保留目录")
            expected[relative] = digest
        observed = {
            path.relative_to(bundle).as_posix() for path in bundle_files
            if path.name != "checksums.sha256"
        }
        if set(expected) != observed:
            raise RunError("Run bundle 文件与校验清单不一致；保留目录")
        for relative, digest in expected.items():
            if hashlib.sha256((bundle / relative).read_bytes()).hexdigest() != digest:
                raise RunError("Run bundle 校验和不匹配；保留目录")
    archive = root.with_suffix(".zip")
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED) as output:
        for path in files:
            output.writestr(
                f"{root.name}/{path.name}",
                _redact(path.read_text(), secrets),
            )
        for path in bundle_files:
            # Rust has already applied share-safe redaction and hashed the
            # bytes; replacing content would invalidate checksums.
            output.write(path, f"{root.name}/{path.relative_to(root)}")
    with zipfile.ZipFile(archive) as verified:
        if verified.testzip() is not None:
            raise RunError("ZIP 完整性检查失败；保留本次报告目录")
    return archive


def _write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2))


def _export(client, run_id, root):
    try:
        manifest = client.json(
            "POST", "/control/store/export-run-bundle",
            {"run_id": run_id, "target": str(root / "bundle")}, timeout=300,
        )
        status_file = root / "bundle/EXPORT_STATUS"
        status = status_file.read_text().strip() if status_file.is_file() else "unavailable"
        if status != (manifest.get("integrity") or {}).get("status"):
            raise RunError("Core 的 bundle 状态与导出文件不一致")
        return status, manifest
    except RunError as error:
        return "unavailable", {"error": str(error)}


def _export_issue(manifest):
    integrity = manifest.get("integrity") or {}
    # Expected authorization omissions are still reported as partial, but
    # they do not undo a completed formal Run. Missing/corrupt facts do.
    return (bool(manifest.get("error") or manifest.get("missing"))
            or any(integrity.get(field, 0) for field in
                   ("unknown_after_crash_calls", "corruption_or_missing_blobs", "dropped_records")))


def run(config_path=None, *, paper=False, keep_artifacts=False, artifact_parent=None,
        observation_seconds=1800, poll_interval=10):
    os.umask(0o077)
    secrets = []
    try:
        client, config = _connection(config_path)
        secrets = _secrets(config, client.token)
        ready = client.json("GET", "/ready", timeout=10)
        if not isinstance(ready, dict) or ready.get("status") != "ok":
            raise RunError("正式 Core 尚未 ready；未提交 Run")
        if ready.get("store_scope") != "canonical":
            raise RunError("当前 Core 不是正式 canonical Store；未提交 Run")
        if ready.get("formal_run_bundle_supported") is not True:
            raise RunError("运行中的 Core 版本过旧；请更新并重启正式 Core 后再提交 Run")
        if paper and not (config["daemon"].get("manual_paper", False)
                          or config["daemon"].get("auto_paper", False)):
            raise RunError("正式 Core 未启用 Paper 手动启动；未提交 Run")
    except (RunError, KeyError, ValueError, OSError, tomllib.TOMLDecodeError) as error:
        print("FINAL_STATUS=not_started", flush=True)
        print(f"ERROR={_redact(str(error), secrets)}", file=sys.stderr, flush=True)
        return 1

    parent = Path(artifact_parent) if artifact_parent else Path(__file__).resolve().parent.parent / ".akzio"
    parent.mkdir(parents=True, exist_ok=True)
    mode = "paper" if paper else "position-plan"
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = Path(tempfile.mkdtemp(prefix=f"{mode}-{stamp}-", dir=parent))
    print(f"ARTIFACT_ROOT={root}", flush=True)
    purpose = "paper" if paper else "position_plan"
    run_id = None
    final_status = "not_started"
    exit_code = 1
    try:
        # The non-idempotent PositionPlan POST must never be retried after a
        # lost response. Paper reuse is owned by Rust's session slot.
        submitted = client.json("POST", "/runs", {"purpose": purpose}, timeout=60)
        run_id = submitted.get("run_id") if isinstance(submitted, dict) else None
        if not isinstance(run_id, str) or not run_id:
            raise AmbiguousSubmission("Core 未返回 Run ID；请先检查正式 Store，勿直接重试")
        print(f"RUN_ID={run_id}", flush=True)
        deadline = time.monotonic() + observation_seconds
        replay = None
        while True:
            replay = client.json("GET", f"/runs/{quote(run_id, safe='')}/replay")
            if replay.get("run_id") != run_id or replay.get("purpose") != purpose:
                raise RunError("Core replay 与提交的 Run ID/purpose 不一致；拒绝推断结果")
            state = replay.get("status")
            if not paper and state == "completed_with_execution_rejection":
                raise RunError("PositionPlan 不包含 Execution；Core 状态不匹配")
            lifecycle = replay.get("lifecycle") or {}
            if (state in TERMINAL or (not paper and state == "decision_completed")
                    or (paper and lifecycle.get("outcome_scheduled") is True)):
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_interval)
        if paper and replay.get("lifecycle", {}).get("outcome_scheduled") and replay["status"] not in TERMINAL:
            final_status = "t0_completed_outcome_pending"
        elif replay.get("status") in TERMINAL or (not paper and replay.get("status") == "decision_completed"):
            final_status = replay["status"]
        else:
            final_status = "pending"
        try:
            detail = client.json("GET", f"/v1/observer/runs/{quote(run_id, safe='')}")
            if (detail.get("workflow") or {}).get("run", {}).get("run_id") != run_id or (
                    (detail.get("workflow") or {}).get("run", {}).get("purpose") != purpose):
                raise RunError("Core Observer 与提交的 Run ID/purpose 不一致；拒绝展示结果")
        except RunError as error:
            detail = None
            print(f"OBSERVATION_STATUS=unavailable ({_redact(str(error), secrets)})", flush=True)
        summary = _summary(run_id, purpose, replay, detail)
        summary["final_status"] = final_status
        _write_json(root / "summary.json", summary)
        print(f"FINAL_STATUS={final_status}", flush=True)
        print(f"RESEARCH_PROPOSAL_STATUS={summary['research']['review_status'] or 'not_observed'}", flush=True)
        proposal_ref = summary["research"]["proposal"] or {}
        print(f"RESEARCH_PROPOSAL_ARTIFACT_ID={proposal_ref.get('artifact_id') or 'not_observed'}", flush=True)
        print(f"RESEARCH_PLAN={json.dumps(summary['decision']['research_plan'], ensure_ascii=False)}", flush=True)
        print(f"DECISION_STATUS={summary['decision']['status']}", flush=True)
        print(f"DECISION_TARGETS={json.dumps(summary['decision']['targets'], ensure_ascii=False)}", flush=True)
        print(f"EXECUTION_VERDICT={summary['execution_verdict']['status']}", flush=True)
        print(f"PAPER_ORDER_SUBMISSION={summary['order_submission']}", flush=True)
        print(f"PAPER_FILL={json.dumps(summary['paper_fills'], ensure_ascii=False)}", flush=True)
        print(f"OUTCOME_SCHEDULE={summary['outcome_schedule']['status']}", flush=True)
        print(f"OUTCOME_SCHEDULED={summary['lifecycle']['outcome_scheduled']}", flush=True)
        print(f"NUMERIC_OUTCOME_SEALED={summary['lifecycle']['numeric_outcome_sealed']}", flush=True)
        print(f"RETROSPECTIVE_STATUS={json.dumps(summary['lifecycle']['retrospective_status'], ensure_ascii=False)}", flush=True)
        export_status, manifest = _export(client, run_id, root)
        print(f"EXPORT_STATUS={export_status}", flush=True)
        if (final_status in SUCCESS or final_status == "t0_completed_outcome_pending"
                or (not paper and final_status == "decision_completed")):
            observed_plan = (paper or (summary["decision"]["status"] == "observed"
                                       and summary["decision"]["research_plan"] is not None))
            exit_code = 0 if detail is not None and observed_plan and not _export_issue(manifest) else 2
        elif final_status == "pending":
            exit_code = 3
    except AmbiguousSubmission as error:
        final_status = "unknown_after_post"
        print(f"FINAL_STATUS={final_status}", flush=True)
        print(f"ERROR={_redact(str(error), secrets)}", file=sys.stderr, flush=True)
    except (RunError, KeyError, ValueError, OSError) as error:
        final_status = "failed_to_observe" if run_id else "not_started"
        print(f"FINAL_STATUS={final_status}", flush=True)
        print(f"ERROR={_redact(str(error), secrets)}", file=sys.stderr, flush=True)
    finally:
        if not (root / "summary.json").is_file():
            _write_json(root / "summary.json", {
                "run_id": run_id, "purpose": purpose, "final_status": final_status,
                "note": "POST may have committed; inspect the formal Store before retrying"
                        if final_status == "unknown_after_post" else None,
            })
        try:
            archive = _archive(root, secrets)
            print(f"ZIP={archive}", flush=True)
            if not keep_artifacts:
                shutil.rmtree(root)
            else:
                print(f"RETAINED_REPORT_ROOT={root}", flush=True)
        except (RunError, OSError, zipfile.BadZipFile) as error:
            print(f"ARCHIVE_ERROR={_redact(str(error), secrets)}", file=sys.stderr, flush=True)
            print(f"RETAINED_REPORT_ROOT={root}", flush=True)
            if exit_code == 0:
                exit_code = 2
    return exit_code


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", nargs="?", help="正式 Core 配置（默认 ~/.akzio/config.toml）")
    parser.add_argument("--mode", choices=("position-plan", "paper"), default="position-plan",
                        help="position-plan: 研究＋Decision、无执行；paper: 正式 Paper scheduler 和全部 Gate")
    parser.add_argument("--keep-artifacts", action="store_true",
                        help="保留本地脱敏报告目录；Run 始终留在正式 Store")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    return run(args.config, paper=args.mode == "paper", keep_artifacts=args.keep_artifacts)


if __name__ == "__main__":
    sys.exit(main())
