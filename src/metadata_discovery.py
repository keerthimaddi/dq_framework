# ============================================================
# DYNAMIC METADATA DISCOVERY
# ============================================================

from typing import List, Dict, Any, Tuple
from pyspark.sql import SparkSession


# Framework/system schemas that should normally not be scanned
DEFAULT_EXCLUDED_SCHEMAS = {
    "information_schema",
    "dqx_bronze",
    "dqx_silver",
    "dqx_gold",
    "dqx_audit",
    "dqx_quarantine",
    "dqx_profiling",
}


def discover_catalogs(
    spark: SparkSession,
    excluded_catalogs: List[str] | None = None
) -> List[str]:
    """
    Discover all catalogs accessible to the current Databricks identity.
    """

    excluded_catalogs = set(excluded_catalogs or [])

    rows = spark.sql("SHOW CATALOGS").collect()

    catalogs = []

    for row in rows:
        catalog = row[0]

        if catalog not in excluded_catalogs:
            catalogs.append(catalog)

    return sorted(catalogs)


def discover_schemas(
    spark: SparkSession,
    catalog: str,
    excluded_schemas: List[str] | None = None
) -> List[str]:
    """
    Discover all schemas inside a catalog.
    """

    excluded = DEFAULT_EXCLUDED_SCHEMAS.copy()

    if excluded_schemas:
        excluded.update(excluded_schemas)

    rows = spark.sql(
        f"SHOW SCHEMAS IN `{catalog}`"
    ).collect()

    schemas = []

    for row in rows:
        schema = row[0]

        if schema not in excluded:
            schemas.append(schema)

    return sorted(schemas)


def list_tables_in_schema(
    spark: SparkSession,
    catalog: str,
    schema: str
) -> List[str]:
    """
    Discover all tables/views available inside a schema.

    This function is responsible for discovering table names
    inside ONE schema.
    """

    try:
        rows = spark.sql(
            f"SHOW TABLES IN `{catalog}`.`{schema}`"
        ).collect()

    except Exception as exc:
        print(
            f"WARNING: Could not inspect "
            f"{catalog}.{schema}: {exc}"
        )
        return []

    tables = []

    for row in rows:
        table_name = row[1]

        # Temporary objects are not part of framework discovery
        is_temporary = False

        if len(row) >= 4:
            is_temporary = bool(row[3])

        if not is_temporary:
            tables.append(table_name)

    return sorted(tables)


def discover_all_metadata(
    spark: SparkSession,
    excluded_catalogs: List[str] | None = None,
    excluded_schemas: List[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Discover:

        catalog
            -> schema
                -> table

    Across ALL catalogs the identity can see.

    Returns one dictionary per table.
    """

    discovered = []

    catalogs = discover_catalogs(
        spark,
        excluded_catalogs
    )

    print("\n" + "=" * 70)
    print("DYNAMIC METADATA DISCOVERY")
    print("=" * 70)

    print(f"Catalogs discovered: {len(catalogs)}")

    for catalog in catalogs:

        print(f"\nCatalog: {catalog}")

        schemas = discover_schemas(
            spark,
            catalog,
            excluded_schemas
        )

        print(f"  Schemas discovered: {len(schemas)}")

        for schema in schemas:

            tables = list_tables_in_schema(
                spark,
                catalog,
                schema
            )

            print(
                f"    {schema}: "
                f"{len(tables)} tables"
            )

            for table in tables:

                full_name = (
                    f"{catalog}.{schema}.{table}"
                )

                discovered.append(
                    {
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                        "full_name": full_name,
                    }
                )

    print(
        f"\nTOTAL TABLES DISCOVERED: "
        f"{len(discovered)}"
    )

    return discovered


def discover_tables(
    spark: SparkSession,
    cfg: Dict[str, Any]
) -> List[Tuple[str, str, str]]:
    """
    Discovers every schema and table inside the single catalog
    configured in dq_rules.yml (framework.catalog).

    Excludes:
        - information_schema
        - framework dqx_* schemas
        - schemas listed under framework.excluded_schemas

    Returns:
        List of (catalog, schema, table) tuples.
    """

    framework = cfg.get("framework", {})
    catalog = framework.get("catalog")

    if not catalog:
        raise ValueError(
            "cfg['framework']['catalog'] is required "
            "for discover_tables()"
        )

    excluded_schemas = framework.get(
        "excluded_schemas",
        []
    )

    print("\n" + "=" * 70)
    print("DATABRICKS CATALOG DISCOVERY")
    print("=" * 70)

    print(f"\nCatalog: {catalog}")

    schemas = discover_schemas(
        spark,
        catalog,
        excluded_schemas
    )

    discovered: List[Tuple[str, str, str]] = []

    for schema in schemas:

        print(f"Discovering schema: {schema}")

        tables = list_tables_in_schema(
            spark,
            catalog,
            schema
        )

        for table in tables:
            discovered.append(
                (catalog, schema, table)
            )

    print("\n" + "=" * 70)
    print("DISCOVERED TABLES")
    print("=" * 70)

    for catalog_name, schema_name, table_name in discovered:
        print(
            f"{catalog_name}."
            f"{schema_name}."
            f"{table_name}"
        )

    print(
        f"\nTotal Tables Found: "
        f"{len(discovered)}"
    )

    return discovered


def get_table_columns(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str
):
    """
    Return the Spark schema for a table.
    """

    return spark.table(
        f"`{catalog}`.`{schema}`.`{table}`"
    ).schema


def get_table_dataframe(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str
):
    """
    Load a dynamically discovered table.
    """

    return spark.table(
        f"`{catalog}`.`{schema}`.`{table}`"
    )


# ============================================================
# SOURCE TABLE DISCOVERY
# ============================================================
#
# Used by main.py when we want to discover only the configured
# source schemas.
#
# Returns:
#
#     (catalog, schema, table)
#
# tuples, which main.py can safely unpack as:
#
#     for catalog, schema, table in tables:
#
# ============================================================

def discover_source_tables(
    spark: SparkSession,
    cfg: Dict[str, Any]
) -> List[Tuple[str, str, str]]:
    """
    Discover source tables under framework.catalog.

    If framework.source_schema_allowlist is configured,
    only schemas in that allowlist are scanned.

    framework.excluded_schemas is also respected.

    Returns:
        A flat list of:
            (catalog, schema, table)
        tuples.
    """

    framework = cfg.get("framework", {})

    catalog = framework.get("catalog")

    if not catalog:
        raise ValueError(
            "cfg['framework']['catalog'] is required "
            "for discover_source_tables()"
        )

    excluded_schemas = framework.get(
        "excluded_schemas",
        []
    )

    allowlist = framework.get(
        "source_schema_allowlist"
    )

    # Discover schemas while respecting the framework's
    # excluded schema configuration.
    schemas = discover_schemas(
        spark,
        catalog,
        excluded_schemas
    )

    # If an allowlist exists, restrict discovery to only
    # those schemas.
    if allowlist:
        schemas = [
            schema
            for schema in schemas
            if schema in allowlist
        ]

    result: List[Tuple[str, str, str]] = []

    # Discover tables inside each selected schema.
    for schema in schemas:

        tables = list_tables_in_schema(
            spark,
            catalog,
            schema
        )

        for table in tables:
            result.append(
                (
                    catalog,
                    schema,
                    table
                )
            )

    print(
        f"Source tables discovered: "
        f"{len(result)} "
        f"(catalog={catalog}, schemas={schemas})"
    )

    return result