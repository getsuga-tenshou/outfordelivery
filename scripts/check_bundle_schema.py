from __future__ import annotations

import glob
import json
import sys

import regex
import yaml
from jsonschema import Draft202012Validator, ValidationError, validators

FILES = ["databricks.yml", *sorted(glob.glob("databricks/resources/*.yml"))]


def unicode_pattern(validator, pattern, instance, schema):
    # The CLI emits Unicode properties (\p{L}, \p{N}) that Python re cannot parse.
    if isinstance(instance, str) and not regex.search(pattern, instance):
        yield ValidationError(f"{instance!r} does not match {pattern!r}")


def main(schema_path: str) -> int:
    with open(schema_path, encoding="utf-8") as fh:
        validator = validators.extend(Draft202012Validator, {"pattern": unicode_pattern})(json.load(fh))
    failed = 0
    for path in FILES:
        with open(path, encoding="utf-8") as fh:
            errors = list(validator.iter_errors(yaml.safe_load(fh)))
        for err in errors:
            print(f"{path}: {'/'.join(map(str, err.path))}: {err.message[:200]}")
        failed += len(errors)
        print(f"{path}: {'ok' if not errors else f'{len(errors)} error(s)'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
