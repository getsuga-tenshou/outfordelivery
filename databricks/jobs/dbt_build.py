from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path


def get_secret(scope: str, key: str, required: bool = True) -> str | None:
    from databricks.sdk.runtime import dbutils

    try:
        return dbutils.secrets.get(scope, key)
    except Exception:
        if required:
            raise
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--secret-scope", required=True)
    args = parser.parse_args()

    repo = Path(args.repo)
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "streaming"))
    from databricks_helpers import dbt_commands
    from dbt.cli.main import dbtRunner
    from streaming.rules import load_time_acceleration

    # The workspace files folder is not a place for dbt's target/ and logs/, so build from a copy.
    work = Path(tempfile.mkdtemp())
    project = work / "dbt"
    shutil.copytree(repo / "dbt", project, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages", "*.duckdb"))

    key_path = work / "snowflake_key.p8"
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(get_secret(args.secret_scope, "snowflake_private_key"))

    os.environ["SNOWFLAKE_ACCOUNT"] = get_secret(args.secret_scope, "snowflake_account")
    os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(key_path)
    passphrase = get_secret(args.secret_scope, "snowflake_private_key_passphrase", required=False)
    if passphrase:
        os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"] = passphrase

    runner = dbtRunner()
    try:
        for command in dbt_commands((project / "packages.yml").exists()):
            print(f"# dbt {' '.join(command)}")
            result = runner.invoke([*command, "--project-dir", str(project), "--profiles-dir", str(project),
                                    "--target", "snowflake", "--vars", json.dumps({
                                        "source_mode": "iceberg", "time_acceleration": load_time_acceleration(repo),
                                    })])
            if not result.success:
                print(f"# dbt {' '.join(command)} failed: {result.exception or 'see log above'}", file=sys.stderr)
                return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    if main():
        raise RuntimeError("Databricks task failed")
