"""Run real research in an isolated Store; optionally use native Alpaca Paper on the formal Paper graph."""
import datetime as dt
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # The original loopback endpoint owns the token; Location grants no
        # authority to receive it, even when the redirected URL is loopback.
        return None


urlopen = build_opener(_NoRedirect()).open


def native_paper_session(root, port):
    """Reuse the Core's provider-calendar projection; Python never classifies sessions."""
    # 只读取隔离 Core 已经校验过的 portfolio 投影；缺失或非字符串都不能用本机日期替代。
    token = (root / 'store/.daemon-token').read_text().strip()
    request = Request(f'http://127.0.0.1:{port}/v1/observer/snapshot',
                      headers={'x-akzio-token': token})
    with urlopen(request, timeout=60) as response:
        snapshot = json.load(response)
    portfolio = snapshot.get('portfolio') or {}
    session = (portfolio.get('data') or {}).get('broker_session')
    if portfolio.get('status') != 'available' or not isinstance(session, str):
        raise RuntimeError('native Paper trading session unavailable; cannot prepare Run')
    dt.date.fromisoformat(session)
    (root / 'paper-session.json').write_text(json.dumps({
        'broker_session': session, 'observed_at': portfolio.get('observed_at'),
        'source': 'Rust Alpaca Paper Clock and Calendar'}, indent=2))
    return session


def run(config_path=None, *, research_only=False, keep_artifacts=False, paper=False):
    # `paper` 与 `research_only` 是互斥的工作边界：前者允许正式 Paper 图，后者在研究复核后停止。
    if paper and research_only:
        raise ValueError('--paper and --research-only select different workflow boundaries')
    os.umask(0o077)
    repo = Path(__file__).resolve().parent.parent
    source = Path(config_path).expanduser() if config_path else Path.home() / '.akzio/config.toml'
    config_text = source.resolve().read_text()
    configuration = tomllib.loads(config_text)
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    artifact_parent = repo / '.akzio'
    artifact_parent.mkdir(exist_ok=True)
    mode = 'paper' if paper else 'position-plan'
    root = Path(tempfile.mkdtemp(prefix=f'{mode}-{stamp}-', dir=artifact_parent))
    print(f'ARTIFACT_ROOT={root}', flush=True)
    cleanup = {'safe': True}
    try:
        # execute 负责实际流程；异常先落盘，finally 再按安全标记决定删除还是保留诊断目录。
        return execute(repo, root, config_text, configuration, source, cleanup,
                       research_only=research_only, paper=paper)
    except Exception as error:
        (root / 'error.json').write_text(json.dumps({'error': str(error)}, indent=2))
        raise
    finally:
        # ZIP 归档在退出路径统一执行；只有 Core 已安全停止且不是 Paper 才允许删除隔离 Store。
        archive_run(root, configuration)
        if cleanup['safe'] and not keep_artifacts and not paper:
            shutil.rmtree(root)
        else:
            print(f'RETAINED_ROOT={root}', flush=True)


def archive_run(root, configuration):
    # The Rust exporter redacts its bundle. Only diagnostics need additional
    # redaction here; keep bundle bytes intact so checksums remain verifiable.
    secrets = set()

    def collect(value):
        # 递归收集键名含凭据语义的配置值；普通配置结构继续向下遍历，不修改原始对象。
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, str) and any(word in key.lower() for word in ('key', 'secret', 'token', 'password')):
                    secrets.add(item)
                    secrets.add(os.path.expandvars(item))
                else:
                    collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    collect(configuration)
    for name, value in os.environ.items():
        if any(word in name for word in ('API_KEY', 'API_SECRET', 'TOKEN', 'PASSWORD')):
            secrets.add(value)
    token = root / 'store/.daemon-token'
    if token.is_file():
        secrets.add(token.read_text().strip())
    secrets.discard('')
    replacements = sorted(secrets | {json.dumps(value)[1:-1] for value in secrets}, key=len, reverse=True)
    archive = root.with_suffix('.zip')
    # 只归档 JSON/日志和 Rust 已生成的 bundle；配置、Store 和 daemon token 不进入 ZIP。
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as output:
        for path in sorted(root.iterdir()):
            if path.is_file() and path.suffix in ('.json', '.log'):
                content = path.read_text(errors='replace')
                for secret in replacements:
                    content = content.replace(secret, '[REDACTED]')
                output.writestr(f'{root.name}/{path.name}', content)
        bundle = root / 'bundle'
        if bundle.is_dir():
            for path in sorted(bundle.rglob('*')):
                if path.is_file():
                    output.write(path, f'{root.name}/{path.relative_to(root)}')
    status_file = root / 'bundle/EXPORT_STATUS'
    status = status_file.read_text().strip() if status_file.is_file() else 'unavailable'
    with zipfile.ZipFile(archive) as verified:
        # 归档创建成功不等于可读；testzip 失败时保留原目录并让异常向上传播。
        if verified.testzip() is not None:
            raise RuntimeError('ZIP integrity check failed; run directory retained')
    print(f'EXPORT_STATUS={status}', flush=True)
    print(f'ZIP={archive}', flush=True)


def migrate_isolated_config(config_text):
    """Remove explicitly retired Planner settings only from this run's copy."""
    # 只在内存中的隔离副本删除退休配置，源文件永不回写；返回删除记录供诊断使用。
    removed = []
    for header in (r'model\.routes\.(?:"research\.planner"|\'research\.planner\')',
                   r'agent\.budget\.planner'):
        config_text, count = re.subn(
            rf'(?ms)^[ \t]*\[{header}\][ \t]*(?:#[^\n]*)?\n.*?(?=^[ \t]*\[|\Z)',
            '', config_text)
        if count:
            removed.append('research.planner route' if header.startswith('model') else 'Planner budget')
    return config_text, removed


def execute(repo, root, config_text, source_configuration, source_config_path, cleanup, *, research_only=False, paper=False):
    # 该函数把配置、Store、Core 和 CLI 都绑定到同一个临时 root；Paper 之外 broker 写入保持禁止。
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    daemon_section = (f'[daemon]\nstore_root = "{root / "store"}"\n'
                      f'http_addr = "127.0.0.1:{port}"\nworker_count = 1\n'
                      f'auto_paper = false\ndebug_control = true\noutcome_processing = {str(paper).lower()}\n'
                      '\n')
    config_text, count = re.subn(
        r'(?ms)^[ \t]*\[daemon\][ \t]*(?:#[^\n]*)?\n.*?(?=^[ \t]*\[|\Z)',
        lambda _: daemon_section, config_text)
    if count != 1:
        raise ValueError('expected exactly one daemon section')
    config_text, removed = migrate_isolated_config(config_text)
    (root / 'config-migration.json').write_text(json.dumps({
        'scope': 'isolated_copy_only', 'removed_retired_settings': removed,
        'source_configuration_modified': False}, indent=2))
    if removed:
        print(f'CONFIG_MIGRATION={json.dumps(removed)}', flush=True)
    config = root / 'runtime.toml'
    # runtime.toml 是本次执行的隔离配置；源配置只用于读取和派生，不会被覆盖。
    config.write_text(config_text)
    binary = repo / 'target' / 'debug' / 'akzio'
    # CLI environment overrides TOML. Keep inherited shell settings from
    # selecting an existing Store or a different build than the one we launch.
    child_environment = os.environ.copy()
    for name in ('AKZIO_MODEL', 'AKZIO_REASONING_EFFORT', 'AKZIO_RESPONSE_LANGUAGE', 'AKZIO_MODEL_ROUTES_JSON'):
        child_environment.pop(name, None)
    child_environment['AKZIO_STORE_ROOT'] = str(root / 'store')
    child_environment['CARGO_TARGET_DIR'] = str(repo / 'target')
    with (root / 'build.log').open('w') as log:
        subprocess.run(['cargo', 'build', '--locked', '--offline', '-p', 'akzio-cli'],
                       cwd=repo, env=child_environment, stdout=log, stderr=subprocess.STDOUT, check=True)
    prefix = [str(binary), '--config', str(config)]

    def command(name, *args, required=True, timeout=60):
        # 每个 CLI 调用保存 stdout/错误输出；required=False 只允许探测 readiness，其他失败向上抛出。
        result = subprocess.run(prefix + list(args), env=child_environment, capture_output=True, text=True, timeout=timeout)
        (root / name).write_text(result.stdout if result.returncode == 0 else result.stdout + result.stderr)
        if required and result.returncode:
            raise RuntimeError(f'{name}: exit {result.returncode}; see saved output')
        return json.loads(result.stdout) if result.returncode == 0 else None

    source_store = Path(source_configuration['daemon']['store_root']).expanduser()
    if not source_store.is_absolute():
        source_store = source_config_path.resolve().parent / source_store
    bootstrap = command('policy-bootstrap.json', 'calibration', 'bootstrap',
                        '--source-store', str(source_store.resolve()),
                        '--target-store', str(root / 'store'))
    if bootstrap.get('status') not in ('bootstrapped', 'unconfigured'):
        raise RuntimeError('unexpected SQL policy bootstrap status; see policy-bootstrap.json')
    policy = command('policy-preflight.json', 'calibration', 'preflight',
                     '--scratch', str(root / 'store'))
    paper_uncalibrated = paper and policy.get('decision_policy_status') == 'store_active_head_missing'
    if policy.get('research_capable') is not True or (not policy.get('decision_capable') and not research_only and not paper_uncalibrated):
        # Rust preflight 不能研究或当前流程不满足 Decision 条件时，在付费模型前结束。
        (root / 'summary.json').write_text(json.dumps({
            'status': 'blocked_before_llm', 'policy': policy, 'bootstrap': bootstrap,
            'broker_write_policy': 'forbidden', 'auto_paper': False,
            'llm_calls': 0, 'broker_writes': 0}, indent=2))
        print('FINAL_STATUS=blocked_before_llm', flush=True)
        return 1

    research_incomplete = research_only or (not policy.get('decision_capable') and not paper)
    if research_incomplete:
        print('PREFLIGHT_STATUS=research_only (stop after final review)', flush=True)
    (root / 'run-mode.json').write_text(json.dumps({
        'mode': 'alpaca_paper' if paper else ('research_only' if research_incomplete else 'position_plan'),
        'decision_capable': policy.get('decision_capable'), 'policy': policy,
        'decision_will_run': not research_incomplete,
        'broker_write_policy': 'paper_allowed' if paper else 'forbidden',
        'external_broker_writes_authorized': paper}, indent=2))

    daemon_log = (root / 'daemon.log').open('w')
    # Popen 只启动隔离 Core；真正的 Run 仍需后续 prepare/resume 或 debug step 明确推进。
    daemon = subprocess.Popen(prefix + ['daemon', 'serve'], env=child_environment, stdout=daemon_log, stderr=subprocess.STDOUT)
    cleanup['safe'] = False
    run_id = None
    try:
        for _ in range(120):
            # readiness 探测不带隐式等待；每次失败最多 sleep 2 秒，超时即保留诊断并失败。
            if daemon.poll() is not None:
                raise RuntimeError('Core exited before readiness')
            if command('ready.json', 'daemon', 'ready', required=False):
                break
            time.sleep(2)
        else:
            raise RuntimeError('Core readiness timed out')
        session = native_paper_session(root, port) if paper else dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')
        prepared = command('prepare.json', 'debug', 'prepare', '--session', session,
                           '--purpose', 'paper' if paper else 'position-plan',
                           *(['--paper-allowed'] if paper else []))
        identity = prepared['identity']
        if (identity['run_purpose'] != ('paper' if paper else 'position_plan')
                or identity['broker_write_policy'] != ('paper_allowed' if paper else 'forbidden')
                or identity['llm_mode'] != 'real'
                or identity.get('decision_policy_status') != policy['decision_policy_status']
                or identity.get('decision_policy_input_hash') != policy['decision_policy_input_hash']
                or (identity.get('decision_policy_artifact') or {}).get('artifact_id') != policy.get('policy_artifact_id')):
            raise RuntimeError('prepared Run does not match real research with native Alpaca Paper' if paper
                               else 'prepared Run is not a real, broker-forbidden PositionPlan')
        run_id = identity['run_id']
        print(f'RUN_ID={run_id}', flush=True)
        if not research_incomplete:
            # 完整 PositionPlan 才 resume 全图；research-only 只逐步推进研究节点，永不调度 Decision。
            command('resume.json', 'debug', 'resume', run_id)
        token = (root / 'store' / '.daemon-token').read_text().strip()
        deadline = time.monotonic() + 1800
        last = None
        while time.monotonic() < deadline:
            # Poll the small persisted lifecycle projection. Full inspect includes
            # every model transcript and should only be exported at the boundary.
            if research_incomplete:
                # 研究模式读取小型节点投影，按 Rust 标记的 step_eligible 推进一个研究节点。
                progress = command('research-progress.json', 'debug', 'nodes', run_id)
                current_nodes = progress['nodes']
                research = progress.get('research', {})
                if research.get('research_status') == 'failed':
                    replay = {'status': 'research_review_failed' if research.get('review_status') == 'failed' else 'research_failed', 'research': research}
                    break
                if research.get('research_status') == 'completed':
                    review_status = research.get('review_status')
                    if review_status == 'accepted':
                        status = 'research_only_policy_missing' if 'policy_missing' in research.get('blocked_reasons', []) else 'research_only_reviewed'
                    else:
                        status = 'research_review_rejected' if review_status == 'rejected' else 'research_review_missing'
                    replay = {'status': status, 'research': research}
                    break
                eligible = [n for n in current_nodes if n.get('step_eligible')
                            and n['task']['node']['recipe_id'] in ('gate.evidence', 'research.analyst', 'research.critic', 'research.supplement', 'research.synthesizer', 'research.proposal_reviewer')]
                if eligible:
                    task = eligible[0]['task']['node']['task_id']
                    print(f'RESEARCH_STEP={eligible[0]["task"]["node"]["recipe_id"]} task={task}', flush=True)
                    command(f'step-{task}.json', 'debug', 'step', run_id, '--task', task, '--wait-seconds', '0', timeout=60)
                time.sleep(2)
                continue
            request = Request(f'http://127.0.0.1:{port}/v1/debug/runs',
                              headers={'x-akzio-token': token})
            with urlopen(request, timeout=30) as response:
                listing = json.load(response)
            current = next(item for item in listing['runs']
                           if item['session']['identity']['run_id'] == run_id)
            (root / 'progress.json').write_text(json.dumps(current, indent=2))
            replay = {'status': current['lifecycle']['execution_status']}
            progress = {'status': replay['status'],
                        'usage': current['lifecycle']['lifecycle_usage']}
            if progress != last:
                print(json.dumps(progress), flush=True)
                last = progress
            if replay['status'] in ('completed', 'failed', 'cancelled', 'completed_with_execution_rejection'):
                # 这些是生命周期终态；accepted/started 等中间状态继续轮询，不被当作完成。
                break
            if daemon.poll() is not None:
                raise RuntimeError('Core exited during Run')
            time.sleep(10)
        else:
            # 到达观测期限时只暂停并保留 Store，返回的 pending 仍需后续 resume/reconcile。
            command('pause.json', 'debug', 'pause', run_id)
            print('OBSERVATION_STATUS=pending; Store retained for resume/reconciliation', flush=True)
        nodes = command('nodes.json', 'debug', 'nodes', run_id)
        doctor = command('doctor.json', 'store', 'doctor')
        command('export.json', 'debug', 'export-bundle', run_id, '--out', str(root / 'bundle'), timeout=300)
        def node_status(recipe):
            return next((n['task']['status'] for n in nodes['nodes']
                         if n['task'].get('node', {}).get('recipe_id') == recipe), 'not_present')
        (root / 'summary.json').write_text(json.dumps({
            'run_id': run_id, 'purpose': identity['run_purpose'],
            'execution_mode': 'alpaca_paper' if paper else 'none',
            'external_broker_writes': None if paper else 0,
            'broker_write_evidence': 'Inspect persisted Paper effect intents and broker receipts' if paper else 'forbidden',
            'status': replay['status'], 'broker_write_policy': identity['broker_write_policy'],
            'decision_policy_status': identity['decision_policy_status'],
            'decision_capable': policy.get('decision_capable'),
            'policy_artifact_id': policy.get('policy_artifact_id'),
            'research': nodes.get('research'),
            'decision_status': 'not_scheduled_research_boundary' if research_incomplete else node_status('gate.decision'),
            'execution_gate_status': node_status('gate.execution'),
            'node_count': len(nodes['nodes']),
            'succeeded_nodes': sum(n['task']['status'] == 'succeeded' for n in nodes['nodes']),
            'doctor': doctor, 'bundle': str(root / 'bundle'),
        }, indent=2))
        print(f'FINAL_STATUS={replay["status"]}', flush=True)
        completed = replay['status'] == 'completed' or (
            paper and replay['status'] == 'completed_with_execution_rejection')
        # Paper 的 execution rejection 仍是正式流程终态；EXPORT_STATUS 再决定是完整还是部分导出。
        if not completed:
            return 3 if paper and replay['status'] not in ('failed', 'cancelled') else 1
        return 0 if (root / 'bundle/EXPORT_STATUS').read_text().strip() == 'complete' else 2
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=30)
            cleanup['safe'] = True
        except subprocess.TimeoutExpired:
            print('Core shutdown still pending; artifacts and process retained', flush=True)
        daemon_log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', nargs='?', help='TOML configuration (default: ~/.akzio/config.toml)')
    parser.add_argument('--research-only', action='store_true', help='Stop after final proposal review with incomplete PositionPlan status, even when Policy is ready; never run Decision')
    parser.add_argument('--keep-artifacts', action='store_true', help='Retain the isolated Store and diagnostics after archiving')
    parser.add_argument('--paper', action='store_true', help='Use native Alpaca Paper on the formal Paper graph, with existing approval and all Gates; retain Store for later reconciliation and Outcome')
    args = parser.parse_args()
    sys.exit(run(args.config, research_only=args.research_only, keep_artifacts=args.keep_artifacts, paper=args.paper))
