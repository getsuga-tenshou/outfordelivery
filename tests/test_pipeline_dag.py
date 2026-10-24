from pathlib import Path

import yaml

from streaming.databricks_helpers import dbt_commands

REPO = Path(__file__).resolve().parent.parent
RESOURCES = REPO / "databricks" / "resources"
JOB = yaml.safe_load((RESOURCES / "pipeline.yml").read_text())["resources"]["jobs"]["outfordelivery_pipeline"]
TASKS = {task["task_key"]: task for task in JOB["tasks"]}


def _upstream(key, seen=None):
    seen = set() if seen is None else seen
    for dep in TASKS[key].get("depends_on", []):
        if dep["task_key"] not in seen:
            seen.add(dep["task_key"])
            _upstream(dep["task_key"], seen)
    return seen


def test_dependencies_exist_and_there_is_no_cycle():
    for key, task in TASKS.items():
        for dep in task.get("depends_on", []):
            assert dep["task_key"] in TASKS
        assert key not in _upstream(key)


def test_every_task_points_at_a_real_script_and_environment():
    environments = {env["environment_key"] for env in JOB["environments"]}
    for task in TASKS.values():
        if "condition_task" in task:
            continue
        assert task["environment_key"] in environments
        assert (RESOURCES / task["spark_python_task"]["python_file"]).resolve().is_file()


def test_order_matches_the_design():
    assert _upstream("bronze_stream") == {"parcels_ingest", "weather_ingest"}
    assert {"bronze_stream", "silver_events", "silver_parcels"} <= _upstream("dbt_build")
    assert {"silver_events", "silver_parcels"} <= _upstream("optimize")
    assert "dbt_build" not in _upstream("optimize")
    assert TASKS["dbt_build"]["depends_on"] == [{"task_key": "dbt_enabled", "outcome": "true"}]
    assert "optimize" in _upstream("dbt_enabled")
    assert TASKS["export_silver"]["depends_on"] == [{"task_key": "optimize"}]
    assert TASKS["dbt_enabled"]["depends_on"] == [{"task_key": "export_silver"}]
    assert TASKS["dbt_enabled"]["condition_task"]["left"] == "${var.run_dbt}"
    assert all(task.get("run_if", "ALL_SUCCESS") == "ALL_SUCCESS" for task in TASKS.values())
    assert "availableNow" in TASKS["bronze_stream"]["spark_python_task"]["parameters"]


def test_job_settings():
    assert JOB["max_concurrent_runs"] == 1
    assert JOB["email_notifications"]["on_failure"]
    assert JOB["schedule"]["quartz_cron_expression"] == "0 0 * * * ?"


def test_free_settings_and_standalone_parameter_forwarding():
    bundle = yaml.safe_load((REPO / "databricks.yml").read_text())
    assert bundle["targets"]["free"]["default"]
    assert bundle["targets"]["free"]["variables"] == {
        "event_source": "volume", "ddb_table": "", "run_dbt": "false", "enable_uniform": "false",
    }
    for path in RESOURCES.glob("*.yml"):
        job = next(iter(yaml.safe_load(path.read_text())["resources"]["jobs"].values()))
        assert job["max_concurrent_runs"] == 1
        for env in job["environments"]:
            assert env["spec"]["environment_version"] == "${var.runtime_version}"
        for task in job["tasks"]:
            if "spark_python_task" not in task:
                continue
            python = task["spark_python_task"]
            args = python["parameters"]
            if Path(python["python_file"]).stem in {"bronze_stream", "parcels_ingest", "weather_ingest"}:
                assert args[args.index("--source") + 1] == "{{job.parameters.source}}"
                assert args[args.index("--batch-id") + 1] == "{{job.parameters.batch_id}}"
            if Path(python["python_file"]).stem == "silver":
                assert args[args.index("--enable-uniform") + 1] == "${var.enable_uniform}"
        if "schedule" in job:
            assert job["schedule"]["pause_status"] == "${var.schedule_pause_status}"


def test_dbt_commands():
    assert dbt_commands(False) == [["source", "freshness"], ["build"]]
    assert dbt_commands(True)[0] == ["deps"]
