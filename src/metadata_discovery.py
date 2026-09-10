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

# ============================================================
# ADD THESE FUNCTIONS TO src/metadata_discovery.py
# (append - do not remove discover_catalogs, discover_schemas,
#  discover_tables, discover_all_metadata, get_table_columns,
#  get_table_dataframe, or discover_source_tables - all still used)
#
# These build on the SAME discover_catalogs/discover_schemas/
# discover_tables primitives discover_source_tables already used
# successfully (proven by real "Source tables discovered" output
# across many runs) - this just widens the loop from one
# configured catalog to every catalog Unity Catalog exposes.
# ============================================================

def discover_all_catalogs(spark, cfg):
    """
    Returns every catalog Unity Catalog exposes, minus whatever is
    listed in framework.excluded_catalogs (system catalogs like
    'system', 'samples', 'hive_metastore' should go there - they
    are NOT source data and should never be profiled/DQ-checked).
    """
    framework = cfg["framework"]
    excluded_catalogs = set(framework.get("excluded_catalogs", []))

    rows = spark.sql("SHOW CATALOGS").collect()
    catalog_col = "catalog" if "catalog" in rows[0].asDict() else rows[0].asDict().keys().__iter__().__next__()
    all_catalogs = [r[catalog_col] for r in rows]

    included = framework.get("included_catalogs")
    if included:
        catalogs = [c for c in all_catalogs if c in included]
    else:
        catalogs = [c for c in all_catalogs if c not in excluded_catalogs]

    print(f"Catalogs discovered: {catalogs} (excluded: {sorted(excluded_catalogs)})")
    return catalogs


def discover_source_tables_all_catalogs(spark, cfg):
    """
    The multi-catalog replacement for discover_source_tables.
    Walks every catalog (per discover_all_catalogs) -> every schema
    in it (excluding information_schema and framework's own
    dqx_* schemas, same as before) -> every table in each schema.

    Returns a flat list of (catalog, schema, table) tuples - same
    shape main.py's `for catalog, schema, table in tables:` loop
    already expects, so main.py's loop body doesn't need to change.

    NOTE: framework.source_schema_allowlist, if set, is still
    honored WITHIN each catalog - e.g. if it's ['demo'], only the
    'demo' schema is scanned in every discovered catalog, not just
    in the framework.catalog one. Leave it empty/unset to scan
    every non-excluded schema in every catalog.
    """
    framework = cfg["framework"]
    excluded_schemas = framework.get("excluded_schemas", [])
    allowlist = framework.get("source_schema_allowlist")

    catalogs = discover_all_catalogs(spark, cfg)
    result = []

    for catalog in catalogs:
        schemas = discover_schemas(spark, catalog, excluded_schemas)
        if allowlist:
            schemas = [s for s in schemas if s in allowlist]

        for schema in schemas:
            for table in discover_tables(spark, catalog, schema):
                result.append((catalog, schema, table))

    print(f"Source tables discovered across {len(catalogs)} catalog(s): {len(result)} total")
    return result


def discover_volumes(spark, catalog, schema):
    """
    Lists Unity Catalog Volumes in a schema (file/unstructured
    storage objects, distinct from tables). Enumeration only -
    volumes hold files, not rows, so the existing row/column-level
    DQ checks don't apply to them directly. This is here so the
    framework at least KNOWS they exist and can log/audit that,
    not to run DQ01-DQ16 against volume contents (that would need
    a separate file-level profiling approach - out of scope unless
    you tell me what "DQ checks on a volume" should mean concretely,
    e.g. checking every file in it parses, or checking file counts).
    """
    try:
        rows = spark.sql(f"SHOW VOLUMES IN `{catalog}`.`{schema}`").collect()
        volumes = [r[0] for r in rows]
    except Exception as exc:
        print(f"Could not list volumes in {catalog}.{schema}: {exc}")
        volumes = []
    return volumes