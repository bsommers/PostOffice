#!/usr/bin/env python3
# INCIDENTAL TOOL: analyze_function_reachability.py · 2026-10-18
# Context: /swe-skills:test-coverage audit & P0/P1 remediation
# Purpose: Computes AST function/method reachability by intersecting Python AST FunctionDef line ranges with coverage.py executed lines.
# Usage: PYTHONPATH=src .venv/bin/python scripts/incidental/analyze_function_reachability.py [--json coverage.json]
# Archaeology: Revealed that 6 of 11 initially unreached functions were abstract pass stubs in BaseClient and BaseSchemaRegistry, and exposed unreached ingress callbacks (_on_message, _ack, _nack) in AmqpClient and MqttClient.

import argparse
import ast
import glob
import json
import os
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute AST function/method reachability from a coverage.py JSON report."
    )
    parser.add_argument(
        "--json",
        default="coverage.json",
        help="Path to coverage.py JSON report (generate with: .venv/bin/coverage json)",
    )
    parser.add_argument(
        "--source",
        default="src/postoffice",
        help="Source directory to scan for Python AST function definitions",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not os.path.exists(args.json):
        print(
            f"Error: {args.json} not found. Run '.venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage json' first.",
            file=sys.stderr,
        )
        return 1

    with open(args.json, "r", encoding="utf-8") as f:
        cov = json.load(f)

    total_funcs = 0
    reached_funcs = 0
    unreached: list[tuple[str, str, int]] = []

    for path in sorted(glob.glob(f"{args.source}/**/*.py", recursive=True)):
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=path)
        file_cov = cov["files"].get(path, {})
        executed = set(file_cov.get("executed_lines", []))

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                total_funcs += 1
                body_lines = set(range(node.body[0].lineno, (node.end_lineno or node.lineno) + 1))
                if body_lines & executed:
                    reached_funcs += 1
                else:
                    unreached.append((path, node.name, node.lineno))

    pct = (reached_funcs / total_funcs * 100.0) if total_funcs else 100.0
    print(f"Functions reached: {reached_funcs}/{total_funcs} ({pct:.1f}%)")
    if unreached:
        print("Unreached functions:")
        for p, name, line in unreached:
            print(f"  {p}:{line} -> {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
