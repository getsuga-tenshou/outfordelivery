import re
from pathlib import Path

from streaming.databricks_helpers import (
    SILVER_EVENT_COLUMNS,
    SILVER_PARCEL_COLUMNS,
    parcels_merge_sql,
    silver_event_select,
    silver_events_ddl,
    silver_parcels_ddl,
)

REPO = Path(__file__).resolve().parent.parent


def test_event_columns_follow_the_dbt_staging_contract():
    sql = (REPO / "dbt" / "models" / "staging" / "stg_parcel_events.sql").read_text()
    staged = re.findall(r"^\s*(?:cast\(\w+ as \w+\) as )?(\w+),?$", sql.split("from")[0], re.M)
    assert [name for name, _, _ in SILVER_EVENT_COLUMNS] == [c for c in staged if c != "select"]


def test_parcel_columns_follow_the_dbt_staging_contract():
    sql = (REPO / "dbt" / "models" / "staging" / "stg_parcels.sql").read_text()
    staged = re.findall(r"^\s*(?:cast\(\w+ as \w+\) as )?(\w+),?$", sql.split("from")[0], re.M)
    assert [name for name, _, _ in SILVER_PARCEL_COLUMNS] == [c for c in staged if c != "select"]


def test_ddl_enables_uniform_and_comments():
    for ddl in (silver_events_ddl("c"), silver_parcels_ddl("c")):
        assert "'delta.universalFormat.enabledFormats' = 'iceberg'" in ddl
        assert "'delta.enableIcebergCompatV2' = 'true'" in ddl
        assert "'delta.enableDeletionVectors' = 'false'" in ddl
        assert "COMMENT '" in ddl
    assert "PARTITIONED BY (event_date)" in silver_events_ddl("c")
    assert "PARTITIONED BY" not in silver_parcels_ddl("c")


def test_select_covers_every_event_column_in_order():
    select = silver_event_select()
    assert [expr.rsplit(" AS ", 1)[1] for expr in select] == [name for name, _, _ in SILVER_EVENT_COLUMNS]


def test_parcels_merge_is_idempotent_by_construction():
    sql = parcels_merge_sql("c")
    assert "s.created_at > t.created_at" in sql
    assert "ON t.parcel_id = s.parcel_id" in sql
