# ============================================================
# CREATE DUMMY CAMPAIGN DATA
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType
)

from datetime import date


CATALOG = "wmg"
SCHEMA = "demo"


spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`"
)


# ============================================================
# CAMPAIGN DATA
# ============================================================

rows = [

    (
        "CAMP_0001",
        "CMP_001",
        "Summer Sale 2026",
        "Prospecting - 18-24",
        1250.50,
        "18-24",
        "Video",
        date(2026, 8, 1),
        "APE DV360 failed",
        12000.0,
        "DV360",
        "L01",
        "MTTD",
        1
    ),

    (
        "CAMP_0002",
        "CMP_002",
        "Back to School",
        "Parents - Display",
        980.25,
        "25-34",
        "Display",
        date(2026, 8, 2),
        "Sigma Deal delayed",
        3000.0,
        "DV360",
        "L02",
        "MTTA",
        1
    ),

    (
        "CAMP_0003",
        "CMP_003",
        "Holiday Preview",
        "High Value Shoppers",
        2100.00,
        "35-44",
        "Carousel",
        date(2026, 8, 3),
        "Creative approval issue",
        5000.0,
        "Meta",
        "L03",
        "MTTR",
        1
    ),

    (
        "CAMP_0004",
        "CMP_004",
        "Always On Retail",
        "Retargeting",
        450.75,
        "45-54",
        "Static",
        date(2026, 8, 4),
        "No incident",
        0.0,
        "Meta",
        "L04",
        "CTR",
        0
    ),

    (
        "CAMP_0005",
        "CMP_005",
        "New Customer Push",
        "Lookalike",
        1750.00,
        "55+",
        "HTML5",
        date(2026, 8, 5),
        "Budget pacing alert",
        3000.0,
        "TTD",
        "L05",
        "Spend",
        1
    ),

    (
        "CAMP_0006",
        "CMP_006",
        "Weekend Flash",
        "Shoppers",
        800.10,
        "25-34",
        "Video",
        date(2026, 8, 6),
        "No incident",
        0.0,
        "DV360",
        "L06",
        "CTR",
        0
    ),

    (
        "CAMP_0007",
        "CMP_007",
        "Summer Sale 2026",
        "Competitor",
        1325.20,
        "35-44",
        "Display",
        date(2026, 8, 7),
        "Data feed late",
        1800.0,
        "Meta",
        "L07",
        "AIDR",
        1
    ),

    (
        "CAMP_0008",
        "CMP_008",
        "Healthy Choice",
        "Category",
        620.40,
        "18-24",
        "Static",
        date(2026, 8, 8),
        "No incident",
        0.0,
        "Meta",
        "L08",
        "Conversion",
        0
    ),

    (
        "CAMP_0009",
        "CMP_009",
        "Holiday Preview",
        "Retargeting",
        1900.00,
        "45-54",
        "Video",
        date(2026, 8, 9),
        "Tracking issue",
        2200.0,
        "DV360",
        "L09",
        "Clicks",
        1
    ),

    (
        "CAMP_0010",
        "CMP_010",
        "Brand Awareness",
        "Prospecting",
        1100.00,
        "55+",
        "Carousel",
        date(2026, 8, 10),
        "No incident",
        0.0,
        "TTD",
        "L10",
        "Impressions",
        0
    ),

    (
        "CAMP_0011",
        "CMP_011",
        "Q3 Promo",
        "Shoppers",
        1400.75,
        "25-34",
        "Video",
        date(2026, 8, 11),
        "API incident",
        4500.0,
        "DV360",
        "L11",
        "MTTD",
        1
    ),

    (
        "CAMP_0012",
        "CMP_012",
        "Q3 Promo",
        "Competitor",
        1550.25,
        "35-44",
        "Display",
        date(2026, 8, 12),
        "No incident",
        0.0,
        "Meta",
        "L12",
        "Spend",
        0
    ),

    (
        "CAMP_0013",
        "CMP_013",
        "Loyalty Drive",
        "Category",
        700.00,
        "45-54",
        "Static",
        date(2026, 8, 13),
        "No incident",
        0.0,
        "Meta",
        "L13",
        "CTR",
        0
    ),

    (
        "CAMP_0014",
        "CMP_014",
        "Retail Media Push",
        "Retargeting",
        2300.00,
        "55+",
        "HTML5",
        date(2026, 8, 14),
        "Reporting failure",
        3100.0,
        "DV360",
        "L14",
        "MTTR",
        1
    ),

    (
        "CAMP_0015",
        "CMP_015",
        "Holiday Preview",
        "Lookalike",
        1800.00,
        "18-24",
        "Video",
        date(2026, 8, 15),
        "No incident",
        0.0,
        "TTD",
        "L15",
        "AIDR",
        0
    ),

    (
        "CAMP_0016",
        "CMP_016",
        "Back to School",
        "Parents",
        950.00,
        "25-34",
        "Display",
        date(2026, 8, 16),
        "Data delay",
        1200.0,
        "DV360",
        "L16",
        "MTTA",
        1
    ),

    (
        "CAMP_0017",
        "CMP_017",
        "Always On Retail",
        "Shoppers",
        510.00,
        "35-44",
        "Carousel",
        date(2026, 8, 17),
        "No incident",
        0.0,
        "Meta",
        "L17",
        "CTR",
        0
    ),

    (
        "CAMP_0018",
        "CMP_018",
        "Brand Awareness",
        "Prospecting",
        1250.00,
        "45-54",
        "Video",
        date(2026, 8, 18),
        "No incident",
        0.0,
        "DV360",
        "L18",
        "Impressions",
        0
    )
]


schema = StructType([

    StructField(
        "record_id",
        StringType(),
        False
    ),

    StructField(
        "campaign_id",
        StringType(),
        False
    ),

    StructField(
        "campaign_name",
        StringType(),
        False
    ),

    StructField(
        "ad_set_name",
        StringType(),
        False
    ),

    StructField(
        "amount_spent_usd",
        DoubleType(),
        False
    ),

    StructField(
        "age_group",
        StringType(),
        False
    ),

    StructField(
        "creative_type",
        StringType(),
        False
    ),

    StructField(
        "event_date",
        DateType(),
        False
    ),

    StructField(
        "incident",
        StringType(),
        False
    ),

    StructField(
        "rev_impact",
        DoubleType(),
        False
    ),

    StructField(
        "ds",
        StringType(),
        False
    ),

    StructField(
        "label_code",
        StringType(),
        False
    ),

    StructField(
        "kpi",
        StringType(),
        False
    ),

    StructField(
        "include_count",
        IntegerType(),
        False
    )
])


df = spark.createDataFrame(
    rows,
    schema
)


# ============================================================
# WRITE CAMPAIGN TABLE
# ============================================================

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`campaign_data`"
    )


# ============================================================
# CAMPAIGN MASTER
# ============================================================

master_rows = [
    ("CMP_001", "Summer Sale 2026"),
    ("CMP_002", "Back to School"),
    ("CMP_003", "Holiday Preview"),
    ("CMP_004", "Always On Retail"),
    ("CMP_005", "New Customer Push"),
    ("CMP_006", "Weekend Flash"),
    ("CMP_007", "Summer Sale 2026"),
    ("CMP_008", "Healthy Choice"),
    ("CMP_009", "Holiday Preview"),
    ("CMP_010", "Brand Awareness"),
    ("CMP_011", "Q3 Promo"),
    ("CMP_012", "Q3 Promo"),
    ("CMP_013", "Loyalty Drive"),
    ("CMP_014", "Retail Media Push"),
    ("CMP_015", "Holiday Preview"),
    ("CMP_016", "Back to School"),
    ("CMP_017", "Always On Retail"),
    ("CMP_018", "Brand Awareness")
]


master_schema = StructType([
    StructField(
        "campaign_id",
        StringType(),
        False
    ),
    StructField(
        "campaign_name",
        StringType(),
        False
    )
])


master_df = spark.createDataFrame(
    master_rows,
    master_schema
)


master_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`campaign_master`"
    )


# ============================================================
# INCIDENT HISTORY
# ============================================================

incident_rows = []

for i in range(1, 31):

    incident_rows.append(
        (
            i,
            float(10 + (i % 15)),
            float(20 + (i % 20)),
            float(30 + (i % 30)),
            float(20 + (i % 50)),
            float(1 + (i % 5)),
            float(500 + i * 175)
        )
    )


incident_schema = StructType([

    StructField(
        "incident_count",
        IntegerType(),
        False
    ),

    StructField(
        "avg_mttd_minutes",
        DoubleType(),
        False
    ),

    StructField(
        "avg_mtta_minutes",
        DoubleType(),
        False
    ),

    StructField(
        "avg_mttr_minutes",
        DoubleType(),
        False
    ),

    StructField(
        "aidr_pct",
        DoubleType(),
        False
    ),

    StructField(
        "severity_score",
        DoubleType(),
        False
    ),

    StructField(
        "total_rev_impact",
        DoubleType(),
        False
    )
])


incident_df = spark.createDataFrame(
    incident_rows,
    incident_schema
)


incident_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        f"`{CATALOG}`.`{SCHEMA}`.`incident_history`"
    )


# ============================================================
# DISPLAY
# ============================================================

print()
print("=" * 60)
print("DUMMY DATA CREATED")
print("=" * 60)

print(
    f"{CATALOG}.{SCHEMA}.campaign_data"
)

print(
    f"{CATALOG}.{SCHEMA}.campaign_master"
)

print(
    f"{CATALOG}.{SCHEMA}.incident_history"
)###

print()
print("CAMPAIGN DATA")
print()

df.show(
    50,
    truncate=False
)