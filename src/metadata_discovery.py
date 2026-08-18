
from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName("Campaign_DQ_Framework").getOrCreate()
    return spark

def discover_tables(spark, catalog_name, excluded_schemas=None):
    excluded = {x.lower() for x in (excluded_schemas or ["information_schema"])}
    print("\n======================================")
    print("DATABRICKS CATALOG DISCOVERY")
    print("======================================")
    print(f"\nCatalog: {catalog_name}")

    catalog_rows = spark.sql("SHOW CATALOGS").collect()
    catalogs = {r[0] for r in catalog_rows}
    if catalog_name not in catalogs:
        raise ValueError(f"Catalog '{catalog_name}' was not found")

    tables = []
    schemas = spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").collect()

    for row in schemas:
        schema_name = row[0]
        if schema_name.lower() in excluded:
            continue

        print(f"\nDiscovering schema: {schema_name}")
        try:
            table_rows = spark.sql(
                f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`"
            ).collect()
        except Exception as e:
            print(f"Skipping schema {schema_name}: {e}")
            continue

        for tr in table_rows:
            table_name = tr[1]
            is_temp = bool(tr[2]) if len(tr) > 2 else False
            if not is_temp:
                tables.append((catalog_name, schema_name, table_name))

    print("\n======================================")
    print("CATALOG / SCHEMA / TABLES")
    print("======================================")
    for c, s, t in tables:
        print(f"Catalog: {c} | Schema: {s} | Table: {t}")
    print(f"\nTotal Tables Found: {len(tables)}")
    return tables
