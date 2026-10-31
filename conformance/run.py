#!/usr/bin/env python3
import argparse
import pathlib
import sys

from postoffice.conformance import run_conformance, write_vectors

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
VECTORS_DIR = REPO_ROOT / "conformance" / "vectors"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="PostOffice Phase 1 Conformance Vector Generator & Runner"
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Regenerate golden JSON vectors in conformance/vectors/",
    )
    parser.add_argument(
        "--vectors-dir",
        type=pathlib.Path,
        default=VECTORS_DIR,
        help="Directory containing conformance JSON vectors",
    )
    args = parser.parse_args(argv)

    if args.generate:
        paths = write_vectors(args.vectors_dir)
        print(f"Generated {len(paths)} conformance suites in {args.vectors_dir}")

    passed, failed, errors = run_conformance(args.vectors_dir)
    for err in errors:
        print(f"FAIL: {err}", file=sys.stderr)
    print(f"Conformance summary: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
