from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--secret-scope", required=True)
    args = parser.parse_args()

    repo = Path(args.repo)
    sys.path.insert(0, str(repo / "streaming"))
    from rules import load_rules

    rules = load_rules(repo)
    print(f"# catalog={args.catalog} secret_scope={args.secret_scope}")
    print(f"# hubs ({len(rules.hub_ids)}): {', '.join(rules.hub_ids)}")
    print(f"# bbox: {rules.bbox}")
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
