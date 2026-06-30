from __future__ import annotations


def test_ingestion_package_exports_public_source_wrappers() -> None:
    import t2c_data.features.ingestion as ingestion

    assert hasattr(ingestion, "list_execution_logs_from_source")
    assert hasattr(ingestion, "load_table_ingestion_summary_from_source")
    assert hasattr(ingestion, "load_table_ingestion_detail_from_source")


def test_ingestion_api_module_imports_with_public_package_contract() -> None:
    import t2c_data.api.ingestion  # noqa: F401

