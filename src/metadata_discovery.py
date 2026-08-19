# ============================================================
# METADATA DISCOVERY
# ============================================================

from pyspark.sql import SparkSession


def quote_identifier(identifier):

    return (
        "`"
        + str(identifier).replace("`", "``")
        + "`"
    )


def full_table_name(
    catalog,
    schema,
    table
):

    return (
        f"{quote_identifier(catalog)}."
        f"{quote_identifier(schema)}."
        f"{quote_identifier(table)}"
    )


def discover_tables(
    spark: SparkSession,
    cfg
):

    print()
    print("=" * 60)
    print("DATABRICKS CATALOG DISCOVERY")
    print("=" * 60)

    framework = cfg["framework"]

    catalog = framework["catalog"]

    excluded_schemas = {
        str(x).lower()
        for x in framework.get(
            "excluded_schemas",
            []
        )
    }

    allowlist = {
        str(x).lower()
        for x in framework.get(
            "source_schema_allowlist",
            []
        )
    }

    print()
    print(f"Catalog: {catalog}")

    catalog_exists = any(
        row[0] == catalog
        for row in spark.sql(
            "SHOW CATALOGS"
        ).collect()
    )

    if not catalog_exists:

        raise ValueError(
            f"Catalog '{catalog}' does not exist"
        )

    tables = []

    schema_rows = spark.sql(
        f"SHOW SCHEMAS IN "
        f"{quote_identifier(catalog)}"
    ).collect()

    for row in schema_rows:

        schema = row[0]

        if schema.lower() in excluded_schemas:
            continue

        if (
            allowlist
            and schema.lower() not in allowlist
        ):
            continue

        print(
            f"Discovering schema: {schema}"
        )

        try:

            table_rows = spark.sql(
                f"SHOW TABLES IN "
                f"{quote_identifier(catalog)}."
                f"{quote_identifier(schema)}"
            ).collect()

            for table_row in table_rows:

                table = table_row[1]

                is_temporary = (
                    bool(table_row[2])
                    if len(table_row) > 2
                    else False
                )

                if is_temporary:
                    continue

                tables.append(
                    (
                        catalog,
                        schema,
                        table
                    )
                )

        except Exception as exc:

            print(
                f"Unable to inspect "
                f"{catalog}.{schema}: {exc}"
            )

    print()
    print("=" * 60)
    print("DISCOVERED TABLES")
    print("=" * 60)

    for catalog, schema, table in tables:

        print(
            f"{catalog}.{schema}.{table}"
        )

    print()
    print(
        f"Total Tables Found: {len(tables)}"
    )

    return tables