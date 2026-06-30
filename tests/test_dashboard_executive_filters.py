from __future__ import annotations

from types import SimpleNamespace

from t2c_data.features.dashboard.executive_queries import _filter_options, filter_profiles, normalize_filters


def _table(*, datasource_id: int, datasource_name: str, database_id: int, database_name: str, schema_id: int, schema_name: str):
    return SimpleNamespace(
        datasource_id=datasource_id,
        datasource_name=datasource_name,
        database_id=database_id,
        database_name=database_name,
        schema_id=schema_id,
        schema_name=schema_name,
        domain_name="Comercial",
        owner_name="Ana Costa",
        certification_status="certified",
        dq_score=92.0,
        open_incidents=0,
        critical_open_incidents=0,
        table_name="orders",
        table_fqn=f"{datasource_name}.{database_name}.{schema_name}.orders",
    )


def test_executive_filter_options_keep_source_and_schema_identity() -> None:
    tables = [
        _table(datasource_id=1, datasource_name="warehouse", database_id=10, database_name="analytics", schema_id=100, schema_name="sales"),
        _table(datasource_id=2, datasource_name="staging", database_id=20, database_name="landing", schema_id=200, schema_name="sales"),
    ]

    options = _filter_options(tables)
    scoped_options = _filter_options(tables, schema_tables=[tables[0]])

    assert {item["value"] for item in options["sources"]} == {"1", "2"}
    assert {item["datasource_id"] for item in options["sources"]} == {1, 2}
    assert options["schemas"][0]["datasource_id"] in {1, 2}
    assert len({item["value"] for item in options["schemas"]}) == 2
    assert all(" / " in str(item["label"]) for item in options["schemas"])
    assert [item["value"] for item in scoped_options["schemas"]] == ["1:10:100"]
    assert scoped_options["schemas"][0]["datasource_id"] == 1


def test_executive_filters_scope_by_datasource_and_schema_key() -> None:
    warehouse_sales = _table(datasource_id=1, datasource_name="warehouse", database_id=10, database_name="analytics", schema_id=100, schema_name="sales")
    warehouse_bronze = _table(datasource_id=1, datasource_name="warehouse", database_id=10, database_name="analytics", schema_id=101, schema_name="bronze")
    staging_sales = _table(datasource_id=2, datasource_name="staging", database_id=20, database_name="landing", schema_id=200, schema_name="sales")

    source_filters = normalize_filters(data_source_id=1)
    schema_filters = normalize_filters(data_source_id=1, schema_key="1:10:100")

    assert filter_profiles([warehouse_sales, warehouse_bronze, staging_sales], source_filters) == [warehouse_sales, warehouse_bronze]
    assert filter_profiles([warehouse_sales, warehouse_bronze, staging_sales], schema_filters) == [warehouse_sales]
