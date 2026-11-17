"""Select an installed TS family and run offline conformance smoke checks."""

import argparse
import json
import sys
from pathlib import Path

from omega.application.conformance import run_conformance
from omega.strategies.registry import StrategyRegistry


def main(argv=None, *, input_fn=input):
    parser = argparse.ArgumentParser(description="Offline Trade Strategy module validator")
    parser.add_argument("module", nargs="?", help="TS family ID; omitted opens a selection prompt")
    parser.add_argument("--list", action="store_true", help="list manifests without importing code")
    parser.add_argument("--json", action="store_true", help="emit machine-readable results")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--external-dir", type=Path)
    parser.add_argument(
        "--allow-external",
        action="store_true",
        help="TRUST WARNING: selected external .py executes arbitrary code "
        "with your user permissions",
    )
    parser.add_argument(
        "--configuration", type=Path, help="JSON parameters for the selected family"
    )
    args = parser.parse_args(argv)
    registry = StrategyRegistry(
        args.external_dir or args.root / "ts_modules", trust_external=args.allow_external
    )
    modules = registry.list_modules()
    if args.list:
        if args.json:
            print(json.dumps({"modules": modules}, ensure_ascii=False))
        else:
            print("다음과 같은 Trade Strategy 모듈들이 감지되었습니다:")
            for module in modules:
                print(
                    f"  {module['id']}: {module['label']} [{module['origin']}, {module['status']}]"
                )
        return 0
    selected = args.module
    if selected is None:
        if args.json:
            parser.error("--json requires a module ID or --list")
        print("다음과 같은 Trade Strategy 모듈들이 감지되었습니다:")
        for module in modules:
            print(f"  {module['id']}: {module['label']} [{module['origin']}, {module['status']}]")
        try:
            selected = input_fn("어떤 모듈을 테스트 하시겠습니까? 모듈 이름 입력: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n검증을 취소했습니다.")
            return 2
    if args.allow_external and not args.json:
        print("주의: 외부 TS의 .py 파일과 검증 함수는 현재 사용자 권한으로 임의 코드를 실행합니다.")
    configuration = None
    try:
        if args.configuration:
            if args.configuration.stat().st_size > 65_536:
                raise ValueError("configuration exceeds limit")
            configuration = json.loads(args.configuration.read_text(encoding="utf-8"))
            if not isinstance(configuration, dict):
                raise ValueError("configuration must be an object")
        report = run_conformance(registry, selected, configuration)
    except Exception as exc:
        if args.json:
            print(
                json.dumps(
                    {"module_id": selected, "status": "failed", "error": type(exc).__name__},
                    ensure_ascii=False,
                )
            )
        else:
            print(
                f"검증을 시작하지 못했습니다: {type(exc).__name__}. "
                "모듈 이름·manifest·신뢰 설정을 확인하세요.",
                file=sys.stderr,
            )
        return 1
    if args.json:
        print(json.dumps(report, ensure_ascii=False, allow_nan=False))
    else:
        print(
            f"\n{selected}: {report['status']} — 통과 {report['passed']}, 실패 {report['failed']}"
        )
        for case in report["cases"]:
            print(f"  [{case['status']}] {case['scope']}: {case['id']}")
            if case["status"] == "failed":
                print(f"    {case.get('error', '')}: {case.get('detail', '')}")
        if report["family_validation"] != "provided":
            print("  모듈 고유 검증이 제공되지 않아 전체 결과는 partial입니다.")
        print(
            "실계정 주문 없이 실행한 초기 안전 검증입니다. "
            "전체 회계·복구·legacy 비교 테스트는 별도로 필요합니다."
        )
    return 1 if report["failed"] else 0 if report["status"] == "passed" else 3


if __name__ == "__main__":
    raise SystemExit(main())
