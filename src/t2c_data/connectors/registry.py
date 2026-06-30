from __future__ import annotations

from dataclasses import dataclass

from t2c_data.connectors.base import BaseConnector
from t2c_data.connectors.implementations import (
    BigQueryConnector,
    DatabricksConnector,
    MariaDbConnector,
    MongoConnector,
    MySQLConnector,
    OracleConnector,
    OtherConnector,
    PostgresConnector,
    RedshiftConnector,
    SnowflakeConnector,
    SqliteConnector,
    SqlServerConnector,
)


@dataclass(frozen=True)
class ConnectorDefinition:
    id: str
    label: str
    group: str
    description: str
    connector_cls: type[BaseConnector]
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...] = ()
    secret_fields: tuple[str, ...] = ()
    enabled: bool = True
    order: int = 0

    @property
    def capabilities(self) -> dict[str, bool]:
        return self.connector_cls.capabilities.as_dict()


CONNECTOR_DEFINITIONS: dict[str, ConnectorDefinition] = {
    "postgres": ConnectorDefinition(
        id="postgres",
        label="PostgreSQL",
        group="primary",
        description="Banco relacional open source",
        connector_cls=PostgresConnector,
        required_fields=("host", "database", "username"),
        optional_fields=("port", "ssl_mode", "default_schema"),
        secret_fields=("password",),
        order=1,
    ),
    "mysql": ConnectorDefinition(
        id="mysql",
        label="MySQL",
        group="primary",
        description="Banco relacional amplamente utilizado",
        connector_cls=MySQLConnector,
        required_fields=("host", "database", "username"),
        optional_fields=("port", "ssl_mode"),
        secret_fields=("password",),
        order=2,
    ),
    "sqlserver": ConnectorDefinition(
        id="sqlserver",
        label="SQL Server",
        group="primary",
        description="Banco corporativo Microsoft",
        connector_cls=SqlServerConnector,
        required_fields=("host", "database", "username"),
        optional_fields=("port", "driver", "encrypt", "trust_server_certificate", "default_schema"),
        secret_fields=("password",),
        order=3,
    ),
    "oracle": ConnectorDefinition(
        id="oracle",
        label="Oracle",
        group="primary",
        description="Banco corporativo enterprise",
        connector_cls=OracleConnector,
        required_fields=("host", "service_name", "username"),
        optional_fields=("port", "default_schema"),
        secret_fields=("password",),
        order=4,
    ),
    "mongodb": ConnectorDefinition(
        id="mongodb",
        label="MongoDB",
        group="primary",
        description="Banco orientado a documentos",
        connector_cls=MongoConnector,
        required_fields=("database",),
        optional_fields=(),
        secret_fields=("uri",),
        order=5,
    ),
    "snowflake": ConnectorDefinition(
        id="snowflake",
        label="Snowflake",
        group="primary",
        description="Data warehouse em nuvem",
        connector_cls=SnowflakeConnector,
        required_fields=("account", "user", "warehouse", "database"),
        optional_fields=("schema", "role"),
        secret_fields=("password",),
        order=6,
    ),
    "bigquery": ConnectorDefinition(
        id="bigquery",
        label="BigQuery",
        group="primary",
        description="Analytics serverless do Google",
        connector_cls=BigQueryConnector,
        required_fields=("project_id",),
        optional_fields=("dataset", "use_adc"),
        secret_fields=("service_account_json",),
        order=7,
    ),
    "redshift": ConnectorDefinition(
        id="redshift",
        label="Redshift",
        group="more",
        description="Data warehouse da AWS",
        connector_cls=RedshiftConnector,
        required_fields=("host", "database", "username"),
        optional_fields=("port", "default_schema"),
        secret_fields=("password",),
        order=8,
    ),
    "databricks": ConnectorDefinition(
        id="databricks",
        label="Databricks",
        group="more",
        description="Lakehouse e analytics",
        connector_cls=DatabricksConnector,
        required_fields=("server_hostname", "http_path"),
        optional_fields=("catalog", "schema"),
        secret_fields=("access_token",),
        order=9,
    ),
    "mariadb": ConnectorDefinition(
        id="mariadb",
        label="MariaDB",
        group="more",
        description="Variante relacional do MySQL",
        connector_cls=MariaDbConnector,
        required_fields=("host", "database", "username"),
        optional_fields=("port", "ssl_mode"),
        secret_fields=("password",),
        order=10,
    ),
    "sqlite": ConnectorDefinition(
        id="sqlite",
        label="SQLite",
        group="more",
        description="Banco leve embarcado",
        connector_cls=SqliteConnector,
        required_fields=("file_path",),
        optional_fields=(),
        secret_fields=(),
        order=11,
    ),
    "other": ConnectorDefinition(
        id="other",
        label="Outros",
        group="more",
        description="Fonte personalizada ou futura integração",
        connector_cls=OtherConnector,
        required_fields=(),
        optional_fields=("connection_string",),
        secret_fields=("connection_string",),
        enabled=False,
        order=12,
    ),
}

CONNECTOR_TYPES = tuple(CONNECTOR_DEFINITIONS.keys())
CONNECTOR_METADATA = {
    key: {
        "id": definition.id,
        "label": definition.label,
        "group": definition.group,
        "description": definition.description,
        "required_fields": list(definition.required_fields),
        "optional_fields": list(definition.optional_fields),
        "secret_fields": list(definition.secret_fields),
        "capabilities": definition.capabilities,
        "enabled": definition.enabled,
        "order": definition.order,
    }
    for key, definition in CONNECTOR_DEFINITIONS.items()
}
