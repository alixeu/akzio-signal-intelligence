"""Run and export one real, non-executing PositionPlan in a new isolated Store."""
import datetime as dt
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen


def run(config_path):
    os.umask(0o077)
    repo = Path(__file__).resolve().parent.parent
    config_text = Path(config_path).resolve().read_text()
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    root = repo / '.akzio' / f'position-plan-{stamp}'
    root.mkdir()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    daemon_section = (f'[daemon]\nstore_root = "{root / "store"}"\n'
                      f'http_addr = "127.0.0.1:{port}"\nworker_count = 1\n'
                      'auto_paper = false\ndebug_control = true\noutcome_processing = false\n\n')
    config_text, count = re.subn(r'(?ms)^\[daemon\]\n.*?(?=^\[|\Z)', daemon_section, config_text)
    if count != 1:
        raise ValueError('expected exactly one daemon section')
    config = root / 'runtime.toml'
    config.write_text(config_text)
    binary = repo / 'target' / 'debug' / 'akzio'
    print(f'ARTIFACT_ROOT={root}', flush=True)
    with (root / 'build.log').open('w') as log:
        subprocess.run(['cargo', 'build', '--locked', '--offline', '-p', 'akzio-cli'],
                       cwd=repo, stdout=log, stderr=subprocess.STDOUT, check=True)
    prefix = [str(binary), '--config', str(config)]

    def command(name, *args, required=True, timeout=60):
        result = subprocess.run(prefix + list(args), capture_output=True, text=True, timeout=timeout)
        (root / name).write_text(result.stdout if result.returncode == 0 else result.stdout + result.stderr)
        if required and result.returncode:
            raise RuntimeError(f'{name}: exit {result.returncode}; see saved output')
        return json.loads(result.stdout) if result.returncode == 0 else None

    daemon_log = (root / 'daemon.log').open('w')
    daemon = subprocess.Popen(prefix + ['daemon', 'serve'], stdout=daemon_log, stderr=subprocess.STDOUT)
    run_id = None
    try:
        for _ in range(120):
            if daemon.poll() is not None:
                raise RuntimeError('Core exited before readiness')
            if command('ready.json', 'daemon', 'ready', required=False):
                break
            time.sleep(2)
        else:
            raise RuntimeError('Core readiness timed out')
        session = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d')
        prepared = command('prepare.json', 'debug', 'prepare', '--session', session, '--purpose', 'position-plan')
        identity = prepared['identity']
        assert identity['run_purpose'] == 'position_plan'
        assert identity['broker_write_policy'] == 'forbidden'
        assert identity['llm_mode'] == 'real'
        run_id = identity['run_id']
        print(f'RUN_ID={run_id}', flush=True)
        command('resume.json', 'debug', 'resume', run_id)
        token = (root / 'store' / '.daemon-token').read_text().strip()
        deadline = time.monotonic() + 1800
        last = None
        while time.monotonic() < deadline:
            # Poll the small persisted lifecycle projection. Full inspect includes
            # every model transcript and should only be exported at the boundary.
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
                break
            if daemon.poll() is not None:
                raise RuntimeError('Core exited during Run')
            time.sleep(10)
        else:
            command('pause.json', 'debug', 'pause', run_id)
            raise RuntimeError('Run exceeded 30-minute observation window; paused')
        nodes = command('nodes.json', 'debug', 'nodes', run_id)
        doctor = command('doctor.json', 'store', 'doctor')
        command('export.json', 'debug', 'export-bundle', run_id, '--out', str(root / 'bundle'), timeout=300)
        (root / 'summary.json').write_text(json.dumps({
            'run_id': run_id, 'purpose': identity['run_purpose'],
            'status': replay['status'], 'broker_write_policy': identity['broker_write_policy'],
            'decision_policy_status': identity['decision_policy_status'],
            'node_count': len(nodes['nodes']),
            'succeeded_nodes': sum(n['task']['status'] == 'succeeded' for n in nodes['nodes']),
            'doctor': doctor, 'bundle': str(root / 'bundle'),
        }, indent=2))
        print(f'FINAL_STATUS={replay["status"]}', flush=True)
        return 0 if replay['status'] == 'completed' else 1
    finally:
        daemon.terminate()
        try:
            daemon.wait(timeout=30)
        except subprocess.TimeoutExpired:
            print('Core shutdown still pending; artifacts and process retained', flush=True)
        daemon_log.close()


if __name__ == '__main__':
    sys.exit(run(sys.argv[1]))
