"""Verify documentation checks cover deliverable files, not ignored run evidence."""
from pathlib import Path
import shutil
import os
import subprocess
import tempfile

repo = Path(__file__).resolve().parents[2]
# 在隔离临时 Git 树中分别验证归档、tracked 文档、新文件和删除文件的链接范围。
(repo / '.akzio').mkdir(exist_ok=True)
root = Path(tempfile.mkdtemp(prefix='markdown-check-', dir=repo / '.akzio'))
(root / 'scripts').mkdir()
shutil.copyfile(repo / 'scripts/check_markdown_links.sh', root / 'scripts/check_markdown_links.sh')
environment = dict(os.environ, GIT_CEILING_DIRECTORIES=str(root.parent))
archive = subprocess.run(['bash', str(root / 'scripts/check_markdown_links.sh')],
                         env=environment, capture_output=True, text=True)
assert archive.returncode != 0, 'an archive without a Git inventory must not report success'
subprocess.run(['git', 'init', '-q', str(root)], check=True)
(root / '.gitignore').write_text('/.akzio/\n')
(root / '.akzio').mkdir()
(root / '.akzio/old-evidence.md').write_text('[local report](/not/a/deliverable.md)\n')
(root / 'README.md').write_text('[guide](guide.md)\n')
(root / 'guide.md').write_text('guide\n')
subprocess.run(['git', '-C', str(root), 'add', '.gitignore', 'README.md', 'guide.md'], check=True)

def check():
    # 每次都重新运行真实 shell 检查，避免用上一次结果代替当前文件树。
    return subprocess.run(['bash', str(root / 'scripts/check_markdown_links.sh')], env=environment, capture_output=True, text=True)

result = check()
assert result.returncode == 0, result.stderr
(root / 'new doc.md').write_text('[missing](missing.md)\n')
result = check()
assert result.returncode == 1 and 'new doc.md' in result.stderr, result.stderr
(root / 'new doc.md').write_text('[valid](guide.md)\n')
(root / 'README.md').write_text('[missing](missing.md)\n')
result = check()
assert result.returncode == 1 and 'README.md' in result.stderr, result.stderr
print('PASS: ignored evidence excluded; tracked and new documentation validated')

# A tracked Markdown file can be deleted before staging. It is no longer part
# of the deliverable tree, but links from surviving files must still fail.
(root / 'README.md').write_text('[guide](guide.md)\n')
(root / 'guide.md').unlink()
result = check()
assert result.returncode == 1 and 'README.md' in result.stderr, result.stderr
(root / 'README.md').write_text('No remaining links.\n')
(root / 'new doc.md').write_text('No remaining links.\n')
result = check()
assert result.returncode == 0 and not result.stderr, result.stderr
print('PASS: unstaged deletions are skipped; surviving references still checked')
