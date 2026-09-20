#!/usr/bin/env python3
"""Run the fixed offline catalog and bind each stable ID to an actual test result."""
import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    # 目录中的稳定 case ID 与实际 crate 测试结果逐一绑定；脚本只报告离线证据，不启动真实模型。
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    catalog = json.loads((ROOT / "config/research-quality-cases.json").read_text())
    cases = catalog["cases"]
    assert len(cases) == len({case["id"] for case in cases}) == 24
    for group in ("CONTEXT", "REVIEW", "LESSON"):
        assert sum(case["id"].startswith(f"RQ-{group}-") for case in cases) == 8
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.out or ROOT / ".akzio" / f"research-quality-offline-{stamp}").resolve()
    if not output.is_relative_to(ROOT / ".akzio"):
        raise ValueError("report must be inside workspace .akzio")
    output.mkdir(parents=True, exist_ok=False)
    commands = []
    observed = {}
    for crate in sorted({case["crate"] for case in cases}):
        # 每个 crate 单独记录日志和退出码，避免一个测试通过掩盖另一个 crate 的失败。
        command = ["cargo", "test", "--locked", "-p", crate, "--lib"]
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
        log = output / f"{crate}.log"
        log.write_text(result.stdout + result.stderr)
        commands.append({"command": command, "exit_code": result.returncode, "log": str(log)})
        for name, state in re.findall(r"^test (\S+) \.\.\. (ok|FAILED|ignored)$", result.stdout, re.M):
            observed[(crate, name.split("::")[-1])] = {"test": name, "status": state, "log": str(log)}
    # 未出现在测试输出中的 case 保持 not_observed；只有全部 case 和全部命令都成功才算 passed。
    rows = [{**case, "evidence": observed.get((case["crate"], case["test"]), {"status": "not_observed"})} for case in cases]
    passed = all(row["evidence"]["status"] == "ok" for row in rows) and all(c["exit_code"] == 0 for c in commands)
    report = {"version": 1, "tier": "offline-verified", "passed": passed, "cases": rows, "commands": commands}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": passed, "cases": len(rows), "report": str(output / "report.json")}))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
