"""Check the real-run launcher without starting Cargo, a daemon or a model."""
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile
from unittest.mock import Mock, patch

repo = Path(__file__).resolve().parents[2]
# 测试显式关闭 pyc，并动态加载 launcher；所有临时 Store 都放在工作区 .akzio 下便于检查归档边界。
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('position_plan_run', repo / 'scripts/position_plan_run.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
for retired in ('-fakerOnline', '--faker-online'):
    # 退休的 fake-online 开关必须在参数层被拒绝，不能成为 Paper 写权限的隐式入口。
    rejected = subprocess.run([sys.executable, str(repo / 'scripts/position_plan_run.py'), retired],
                              capture_output=True, text=True)
    assert rejected.returncode == 2 and 'unrecognized arguments' in rejected.stderr
print('PASS: retired fake-online flags cannot become Paper write permission')
(repo / '.akzio').mkdir(exist_ok=True)
fixture = Path(tempfile.mkdtemp(prefix='launcher-check-', dir=repo / '.akzio'))
config = fixture / 'input.toml'
config.write_text('  [daemon] # valid TOML header\nstore_root = "unused"\n[model]\nmodel = "fixture"\n')
(fixture / 'unused').mkdir()
(fixture / 'unused/akzio.sqlite3').touch()
(fixture / 'store').mkdir()
(fixture / 'store/.daemon-token').write_text('test-private-daemon-token')
with patch.object(module, 'urlopen', return_value=io.BytesIO(b'{"portfolio":{"status":"unavailable"}}')):
    try:
        module.native_paper_session(fixture, 1)
    except RuntimeError as error:
        assert 'trading session unavailable' in str(error)
    else:
        raise AssertionError('missing Core session must not fall back to local date')
daemon = Mock()
daemon.poll.side_effect = [None, 1]
ready_policy = {'research_capable': True, 'decision_capable': True, 'decision_policy_status': 'ready_for_current_decision',
                'decision_policy_input_hash': 'a' * 64, 'contract_match': True,
                'policy_artifact_id': 'b' * 64}
uncalibrated_policy = {'research_capable': True, 'decision_capable': False,
                      'decision_policy_status': 'store_active_head_missing',
                      'decision_policy_input_hash': None, 'contract_match': None,
                      'policy_artifact_id': None}

def capture_runtime_config(args, **kwargs):
    # 用假的 Popen 捕获实际传给 Core 的隔离配置，同时让 readiness 分支观察到 Core 已退出。
    global runtime_config
    runtime_config = Path(args[2]).read_text()
    return daemon

with patch.dict(os.environ, {'AKZIO_STORE_ROOT': '/must-not-open', 'CARGO_TARGET_DIR': '/old-build'}), \
     patch.object(module.subprocess, 'run', side_effect=[subprocess.CompletedProcess([], 0, '{}', ''),
         subprocess.CompletedProcess([], 0, '{"status":"bootstrapped"}', ''),
         subprocess.CompletedProcess([], 0, json.dumps(ready_policy), ''),
         subprocess.CompletedProcess([], 0, '{}', '')]) as run, \
     patch.object(module.subprocess, 'Popen', side_effect=capture_runtime_config) as popen, \
     patch.object(module.time, 'sleep'):
    try:
        module.run(config)
    except RuntimeError as error:
        assert str(error) == 'Core exited before readiness'
    else:
        raise AssertionError('fake Core was expected to stop before execution')
    env = popen.call_args.kwargs['env']
    store = Path(env['AKZIO_STORE_ROOT'])
    assert store.parent.parent == repo / '.akzio' and store.name == 'store'
    assert env['CARGO_TARGET_DIR'] == str(repo / 'target')
    assert len(run.call_args_list) == 4  # build, policy bootstrap/preflight, readiness CLI
    assert all(call.kwargs['env'] == env for call in run.call_args_list)
    assert not store.parent.exists()
    assert store.parent.with_suffix('.zip').is_file()
    assert f'store_root = "{store}"' in runtime_config
    assert 'auto_paper = false' in runtime_config
    assert 'debug_control = true' in runtime_config
print('PASS: build, Core and CLI share the new isolated Store and exact build directory')

# 优化字节码也不能移除 resume 前的身份校验；不匹配时必须在任何执行前失败。
exec(compile((repo / 'scripts/position_plan_run.py').read_text(),
             str(repo / 'scripts/position_plan_run.py'), 'exec', optimize=2), module.__dict__)
roots = set()
frozen_now = module.dt.datetime.now(module.dt.timezone.utc)
for _ in range(2):
    daemon = Mock()
    daemon.poll.return_value = None
    replies = [
        subprocess.CompletedProcess([], 0, '', ''),
        subprocess.CompletedProcess([], 0, '{"status":"bootstrapped"}', ''),
        subprocess.CompletedProcess([], 0, json.dumps(ready_policy), ''),
        subprocess.CompletedProcess([], 0, '{"ready":true}', ''),
        subprocess.CompletedProcess([], 0, '{"identity":{"run_purpose":"paper",'
                                    '"broker_write_policy":"paper_allowed","llm_mode":"real"}}', ''),
    ]
    with patch.object(module.dt, 'datetime') as clock, \
         patch.object(module.subprocess, 'run', side_effect=replies) as run, \
         patch.object(module.subprocess, 'Popen', return_value=daemon) as popen:
        clock.now.return_value = frozen_now
        try:
            module.run(config)
        except RuntimeError as error:
            assert 'broker-forbidden PositionPlan' in str(error), error
        else:
            raise AssertionError('unsafe identity must not reach debug resume')
        assert len(run.call_args_list) == 5
        roots.add(popen.call_args.kwargs['env']['AKZIO_STORE_ROOT'])
        daemon.terminate.assert_called_once()
assert len(roots) == 2, 'same-second invocations must allocate different Stores'
print('PASS: same-second runs are isolated; optimized Python rejects unsafe identity before resume')

# 使用伪造 home 配置和已完成 provider 响应，验证默认路径、脱敏归档和 Paper/PositionPlan 退出边界。
fake_home = fixture / 'home'
(fake_home / '.akzio').mkdir(parents=True)
(fake_home / '.akzio/unused').mkdir()
(fake_home / '.akzio/unused/akzio.sqlite3').touch()
source = fake_home / '.akzio/config.toml'
source_text = config.read_text() + 'api_key = "test-private-api-key"\n[model.routes."research.planner"]\nmodel = "retired"\n'
source.write_text(source_text)
for lifecycle_status, export_status, expected_exit, run_policy, paper in [
        ('completed', 'complete', 0, ready_policy, False), ('completed', 'partial', 2, ready_policy, False),
        ('failed', 'complete', 1, ready_policy, False),
        ('completed', 'complete', 0, uncalibrated_policy, True),
        ('completed_with_execution_rejection', 'complete', 0, uncalibrated_policy, True),
        ('running', 'complete', 3, ready_policy, True),
        ('completed_with_execution_rejection', 'complete', 1, ready_policy, False)]:
    daemon = Mock()
    daemon.poll.return_value = None
    run_root = None
    payload = b'{"response":"real provider visible content fixture"}\n'

    def fake_command(args, **kwargs):
        # 根据被测 CLI 子命令返回最小持久化响应；没有真实 Cargo、Core、模型或 Broker I/O。
        global run_root
        run_root = Path(kwargs['env']['AKZIO_STORE_ROOT']).parent
        if args[0] == 'cargo':
            kwargs['stdout'].write('build fixture\n')
            return subprocess.CompletedProcess(args, 0, '', '')
        action = args[3:5]
        result = {}
        if action == ['calibration', 'preflight']:
            result = run_policy
        elif action == ['calibration', 'bootstrap']:
            result = {'status': 'bootstrapped' if run_policy['decision_capable'] else 'unconfigured'}
        elif action == ['daemon', 'ready']:
            (run_root / 'store').mkdir(exist_ok=True)
            (run_root / 'store/.daemon-token').write_text('test-private-daemon-token')
            result = {'ready': True}
        elif action == ['debug', 'prepare']:
            assert args[args.index('--purpose')+1] == ('paper' if paper else 'position-plan')
            assert ('--paper-allowed' in args) is paper
            if paper:
                assert args[args.index('--session')+1] == '2031-01-07', 'use Core trade date, not host date'
            isolated = module.tomllib.loads((run_root / 'runtime.toml').read_text())
            assert 'faker_online' not in isolated['daemon']
            assert isolated['daemon']['auto_paper'] is False
            assert isolated['daemon']['outcome_processing'] is paper
            assert 'research.planner' not in isolated['model'].get('routes', {})
            result = {'identity': {'run_id': 'test-run', 'run_purpose': 'paper' if paper else 'position_plan',
                                  'broker_write_policy': 'paper_allowed' if paper else 'forbidden', 'llm_mode': 'real',
                                  'decision_policy_status': run_policy['decision_policy_status'],
                                  'decision_policy_input_hash': run_policy['decision_policy_input_hash'],
                                  'decision_policy_artifact': {
                                      'artifact_id': 'b' * 64,
                                      'kind': 'decision_policy'} if run_policy['decision_capable'] else None}}
        elif action == ['debug', 'resume']:
            (run_root / 'store').mkdir(exist_ok=True)
            (run_root / 'store/.daemon-token').write_text('test-private-daemon-token')
            (run_root / 'store/private.db').write_bytes(b'private store')
        elif action == ['debug', 'nodes']:
            result = {'nodes': [{'task': {'status': 'succeeded'}}]}
        elif action == ['debug', 'export-bundle']:
            bundle = run_root / 'bundle'
            bundle.mkdir()
            (bundle / 'EXPORT_STATUS').write_text(export_status + '\n')
            (bundle / 'llm_calls.jsonl').write_bytes(payload)
            (bundle / 'checksums.sha256').write_text(hashlib.sha256(payload).hexdigest() + '  llm_calls.jsonl\n')
        return subprocess.CompletedProcess(args, 0, json.dumps(result), '')

    def fake_daemon(*args, **kwargs):
        # 日志故意写入凭据样本，随后由 archive_run 验证它们已被脱敏。
        log = kwargs['stdout']
        log.write('test-private-api-key test-private-daemon-token\n')
        daemon.wait.side_effect = lambda **kw: log.write('shutdown finished\n')
        return daemon

    listing = {'runs': [{'session': {'identity': {'run_id': 'test-run'}},
                         'lifecycle': {'execution_status': lifecycle_status, 'lifecycle_usage': {}}}]}
    def provider_response(request, **kwargs):
        # Paper session 只能来自 Rust portfolio projection；其他请求返回固定 Run 生命周期列表。
        payload = {'portfolio': {'status': 'available', 'data': {'broker_session': '2031-01-07'}}} \
            if request.full_url.endswith('/observer/snapshot') else listing
        return io.BytesIO(json.dumps(payload).encode())

    with patch.object(module.Path, 'home', return_value=fake_home), \
         patch.object(module.subprocess, 'run', side_effect=fake_command), \
         patch.object(module.subprocess, 'Popen', side_effect=fake_daemon), \
         patch.object(module.time, 'monotonic', side_effect=[0, 0, 1801]), \
         patch.object(module.time, 'sleep'), \
         patch.object(module, 'urlopen', side_effect=provider_response):
        assert module.run(paper=paper) == expected_exit
    assert source.read_text() == source_text
    assert run_root.exists() is paper, 'Paper runs retain Store for reconciliation and Outcome'
    assert not (fake_home / '.akzio/store').exists()
    with zipfile.ZipFile(run_root.with_suffix('.zip')) as archive:
        assert archive.testzip() is None
        names = archive.namelist()
        assert not any('/store/' in name or name.endswith('.toml') for name in names)
        assert archive.read(f'{run_root.name}/bundle/llm_calls.jsonl') == payload
        assert 'shutdown finished' in archive.read(f'{run_root.name}/daemon.log').decode()
        for name in names:
            content = archive.read(name)
            assert b'test-private-api-key' not in content
            assert b'test-private-daemon-token' not in content
print('PASS: default home config, automatic ZIP, preserved bundle, redacted logs and terminal exit codes')

daemon = Mock()
daemon.poll.return_value = None
lifecycle_status, export_status, run_policy = 'completed', 'complete', ready_policy
paper = False
listing['runs'][0]['lifecycle']['execution_status'] = lifecycle_status

def pending_daemon(*args, **kwargs):
    # 未确认的 shutdown 必须令 launcher 保留隔离 Store，供后续恢复或对账。
    process = fake_daemon(*args, **kwargs)
    process.wait.side_effect = subprocess.TimeoutExpired('daemon', 30)
    return process

with patch.object(module.Path, 'home', return_value=fake_home), \
     patch.object(module.subprocess, 'run', side_effect=fake_command), \
     patch.object(module.subprocess, 'Popen', side_effect=pending_daemon), \
     patch.object(module, 'urlopen', return_value=io.BytesIO(json.dumps(listing).encode())):
    assert module.run() == 0
assert run_root.is_dir(), 'a running Core must retain its Store'
assert run_root.with_suffix('.zip').is_file()
assert source.read_text() == source_text
print('PASS: unconfirmed Core shutdown retains Store and ZIP')

with patch.object(module.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, ['cargo'])):
    before = set((repo / '.akzio').glob('position-plan-*.zip'))
    try:
        module.run(config)
    except subprocess.CalledProcessError:
        pass
    else:
        raise AssertionError('build failure must propagate')
    created = set((repo / '.akzio').glob('position-plan-*.zip')) - before
    assert len(created) == 1
    archive_path = created.pop()
    assert not archive_path.with_suffix('').exists()
    with zipfile.ZipFile(archive_path) as archive:
        assert any(name.endswith('/error.json') for name in archive.namelist())
print('PASS: build failure still produces a diagnostic ZIP and propagates failure')

# Bootstrap 仍必须到达 Rust preflight；无效 SQL policy 在模型启动前阻断。
# models. Policy identity comes only from the SQL Store.
sql_only_config = fixture / 'sql-only.toml'
sql_only_config.write_text(
    '  [daemon]\nstore_root = "missing-store"\n'
    '[execution]\n'
    '[model]\nmodel = "fixture"\n')
responses = [
    subprocess.CompletedProcess([], 0, '', ''),
    subprocess.CompletedProcess([], 0, json.dumps({
        'status': 'unconfigured', 'source_store_created': True,
        'store_integrity': 'passed'}), ''),
    subprocess.CompletedProcess([], 0, json.dumps({
        'research_capable': False, 'decision_capable': False, 'decision_policy_status': 'invalid_store_policy_or_identity',
        'decision_policy_input_hash': None, 'contract_match': None, 'llm_calls': 0}), ''),
]
with patch.object(module.subprocess, 'run', side_effect=responses) as run, \
     patch.object(module.subprocess, 'Popen') as popen:
    assert module.run(sql_only_config) == 1
    popen.assert_not_called()
    assert len(run.call_args_list) == 3, 'bootstrap must still reach the Rust preflight'
    bootstrap_args = run.call_args_list[1].args[0]
    assert bootstrap_args[3:5] == ['calibration', 'bootstrap']
    assert run.call_args_list[2].args[0][3:5] == ['calibration', 'preflight']
    run_root = Path(run.call_args_list[2].kwargs['env']['AKZIO_STORE_ROOT']).parent
    assert not run_root.exists()
    with zipfile.ZipFile(run_root.with_suffix('.zip')) as archive:
        summary = json.loads(archive.read(f'{run_root.name}/summary.json'))
    assert summary['policy']['contract_match'] is None
    assert summary['policy']['decision_policy_status'] == 'invalid_store_policy_or_identity'
print('PASS: invalid SQL policy stops at Rust preflight before model calls')

# Failed packing or validation must never delete the only diagnostic copy.
for failure in ('write', 'integrity'):
    roots_before = set((repo / '.akzio').glob('position-plan-*'))
    archive_patch = (patch.object(module.zipfile.ZipFile, 'writestr', side_effect=OSError('disk full'))
                     if failure == 'write' else
                     patch.object(module.zipfile.ZipFile, 'testzip', return_value='bad-entry'))
    with archive_patch:
        # execute is mocked, so create a diagnostic for the archive writer.
        def diagnostic(repo, root, *args, **kwargs):
            # 模拟 execute 已产生诊断；归档写入/校验失败时该目录必须保留。
            (root / 'summary.json').write_text('{}')
            return 0
        with patch.object(module, 'execute', side_effect=diagnostic):
            try:
                module.run(config)
            except (OSError, RuntimeError):
                pass
            else:
                raise AssertionError('archive failure must propagate')
    retained = [p for p in set((repo / '.akzio').glob('position-plan-*')) - roots_before if p.is_dir()]
    assert len(retained) == 1
    assert (retained[0] / 'summary.json').read_text() == '{}'
print('PASS: archive write and integrity failures preserve diagnostics')

# 缺少 active policy 仍可能 research-capable，但不是完整 PositionPlan，且不得启动 Core。
with patch.object(module.subprocess, 'run', side_effect=[
    subprocess.CompletedProcess([], 0, '{}', ''),
    subprocess.CompletedProcess([], 0, '{"status":"unconfigured"}', ''),
    subprocess.CompletedProcess([], 0, json.dumps(uncalibrated_policy), '')]), \
     patch.object(module.subprocess, 'Popen') as popen:
    assert module.run(config) == 1
    popen.assert_not_called()
print('PASS: absent policy fails before Core and paid models; research-only requires explicit mode')

# Research-only 即使 policy ready 也在复核后停止；Rust 保留其状态，launcher 不 resume 全图或调度 Decision。
# the launcher never resumes the full graph or schedules Decision here.
for run_policy, review_status, expected_status in [
        (ready_policy, 'accepted', 'research_only_reviewed'),
        (uncalibrated_policy, 'accepted', 'research_only_policy_missing'),
        (ready_policy, 'rejected', 'research_review_rejected'),
        (ready_policy, 'not_reached', 'research_review_missing'),
        (uncalibrated_policy, 'failed', 'research_review_failed')]:
    daemon = Mock()
    daemon.poll.return_value = None
    paper, export_status = False, 'complete'
    research = {'research_status': 'failed' if review_status == 'failed' else 'completed', 'review_status': review_status,
                'blocked_reasons': [] if run_policy['decision_capable'] else ['policy_missing']}
    def research_command(args, **kwargs):
        action = args[3:5]
        assert action not in (['debug', 'resume'], ['debug', 'step']), 'completed research must stop before Decision'
        if action == ['debug', 'nodes']:
            result = {'research': research, 'nodes': [{'task': {'status': 'pending', 'node': {'recipe_id': 'gate.decision'}}}]}
            return subprocess.CompletedProcess(args, 0, json.dumps(result), '')
        return fake_command(args, **kwargs)
    with patch.object(module.Path, 'home', return_value=fake_home), \
         patch.object(module.subprocess, 'run', side_effect=research_command), \
         patch.object(module.subprocess, 'Popen', side_effect=fake_daemon), \
         patch.object(module.time, 'sleep'):
        assert module.run(research_only=True) == 1, 'research-only is not full PositionPlan success'
    with zipfile.ZipFile(run_root.with_suffix('.zip')) as archive:
        summary = json.loads(archive.read(f'{run_root.name}/summary.json'))
    assert summary['status'] == expected_status
    assert summary['research'] == research
    assert summary['decision_status'] == 'not_scheduled_research_boundary'
print('PASS: research-only stops after review with ready/missing Policy; rejected, missing and failed reviews remain distinct')
