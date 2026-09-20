"""Exercise shell entrypoints with local build/export substitutes, without Core I/O."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

repo = Path(__file__).resolve().parents[2]
# 测试树用 stub 替换 swift/cargo/codesign/hdiutil，验证脚本路径和退出边界而不访问真实 Core。
(repo / '.akzio').mkdir(exist_ok=True)
root = Path(tempfile.mkdtemp(prefix='shell-check-', dir=repo / '.akzio'))
for folder in ('scripts', 'apps/Scripts', 'apps/Resources', 'config', 'bin', 'caller'):
    (root / folder).mkdir(parents=True, exist_ok=True)
for name in ('build_app.sh', 'sign_app.sh', 'create_dmg.sh'):
    shutil.copy2(repo / 'apps/Scripts' / name, root / 'apps/Scripts' / name)
for name in ('update_app_and_submit_debug.sh', 'export_debug_bundle.sh'):
    shutil.copy2(repo / 'scripts' / name, root / 'scripts' / name)
(root / 'Cargo.toml').write_text('# fixture manifest\n')
(root / 'config/akzio.observatory.toml').write_text('[daemon]\nauto_paper = false\n')
(root / 'apps/Resources/Info.plist.in').write_text('__VERSION__ __BUILD__')
(root / 'bin/AkzioObservatory').write_text('fixture Swift product')
stub = '''
import json, os, pathlib, sys
root = pathlib.Path(os.environ['CHECK_ROOT'])
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / 'calls.jsonl').open('a') as log:
    log.write(json.dumps([name, args]) + '\\n')
if name == 'swift' and '--show-bin-path' in args:
    print(root / 'bin')
elif name == 'cargo':
    if '--manifest-path' not in args or '--locked' not in args:
        sys.exit('missing explicit manifest or lockfile flag')
    if args[0] == 'build':
        binary = pathlib.Path(os.environ['CARGO_TARGET_DIR']) / 'release/akzio'
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text('fixture Rust product')
    else:
        bundle = pathlib.Path(args[args.index('--out') + 1])
        bundle.mkdir()
        (bundle / 'EXPORT_STATUS').write_text(os.environ.get('CHECK_STATUS', 'complete'))
elif name == 'hdiutil':
    pathlib.Path(args[-1]).write_text('fixture disk image')
'''
for name in ('swift', 'cargo', 'codesign', 'hdiutil'):
    binary = root / 'bin' / name
    binary.write_text(f'#!{sys.executable}\n' + stub)
    binary.chmod(0o755)
environment = dict(os.environ, PATH=str(root / 'bin') + os.pathsep + os.environ['PATH'],
                   CHECK_ROOT=str(root), TMPDIR=str(root), CARGO_TARGET_DIR='custom-target',
                   AKZIO_APP_BUNDLE=str(root / 'apps/dist/check.app'),
                   AKZIO_DMG_PATH=str(root / 'apps/dist/check.dmg'),
                   AKZIO_BIN=str(root / 'missing-binary'))


def run(script, *args, status=0, env=None):
    # 从不同当前目录启动脚本，确认脚本自身解析仓库路径而不依赖调用者 cwd。
    result = subprocess.run(['bash', str(root / script), *args], cwd=root / 'caller',
                            env=env or environment, capture_output=True, text=True)
    assert result.returncode == status, (script, result.returncode, result.stdout, result.stderr)
    return result


run('scripts/update_app_and_submit_debug.sh')
bundle = Path(environment['AKZIO_APP_BUNDLE'])
assert (bundle / 'Contents/MacOS/akzio-core').read_text() == 'fixture Rust product'
assert (root / 'custom-target/release/akzio').exists(), 'build products must survive packaging'
calls_before = (root / 'calls.jsonl').read_text()
run('scripts/update_app_and_submit_debug.sh', status=1)
assert (root / 'calls.jsonl').read_text() == calls_before, 'existing bundle must fail before build'
run('apps/Scripts/create_dmg.sh')
assert Path(environment['AKZIO_DMG_PATH']).exists()
calls_before = (root / 'calls.jsonl').read_text()
run('apps/Scripts/create_dmg.sh', status=1)
assert (root / 'calls.jsonl').read_text() == calls_before, 'existing DMG must fail before signing'
print('PASS: packaging from another directory, relative Cargo target, retained products, overwrite refusal')

# No executable exists: exercise the real Cargo fallback from outside the repo.
run('scripts/export_debug_bundle.sh', '--help')
run('scripts/export_debug_bundle.sh', '--config', status=1)
config = root / 'caller/input.toml'
config.write_text('[daemon]\n')
for export_status, code in (('complete', 0), ('partial', 2)):
    result = run('scripts/export_debug_bundle.sh', '--config', 'input.toml', '--run-id', 'fixture',
                 '--out', export_status, status=code,
                 env=dict(environment, CHECK_STATUS=export_status))
    assert f'status={export_status}' in result.stdout
    assert len(list((root / 'caller' / export_status).glob('*.tar.gz'))) == 1
calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
exports = [args for name, args in calls if name == 'cargo' and args[0] == 'run']
assert len(exports) == 2
assert all(args[args.index('--manifest-path') + 1] == str(root / 'Cargo.toml') for args in exports)
print('PASS: export fallback selects repository manifest; complete/partial archives preserve exit status')
print(f'ARTIFACT_ROOT={root}')
