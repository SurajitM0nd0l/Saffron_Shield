from pyspark import pipelines as dp
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")

# ============================================================
# 1. BRONZE: POLICY CHANGE INGESTION
# ============================================================

@dp.table(
    name=f"{catalog}.bronze.policy_changes",
    comment="Raw policy change events ingested using Auto Loader"
)
def policy_changes():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/claims/raw/landing/policy_change/")
    )


# ============================================================
# 2. SILVER: STANDARDIZE POLICY CHANGE EVENTS
# ============================================================

@dp.temporary_view(name="policy_changes_clean")
def policy_changes_clean():

    policy_df = spark.readStream.table(
        f"{catalog}.bronze.policy_changes"
    )

    return (
        policy_df
        .withColumns({

            "member_id": trim(col("member_id")),
            "policy_id": trim(col("policy_id")),
            "plan_code": trim(col("plan_code")),

            "effective_from": to_date(col("effective_from")),

            "op": upper(trim(col("op"))),

            "op_ts": to_timestamp(col("op_ts"))
        })
        .filter(
            col("member_id").isNotNull()
            & col("policy_id").isNotNull()
            & col("op_ts").isNotNull()
        )
    )


# ============================================================
# 3. DECLARE SCD TYPE 2 TARGET TABLE
# ============================================================

dp.create_streaming_table(
    name=f"{catalog}.silver.policy_scd2",
    comment="Policy history maintained using SCD Type 2"
)


# ============================================================
# 4. APPLY AUTO CDC - SCD TYPE 2
# ============================================================

dp.create_auto_cdc_flow(

    target=f"{catalog}.silver.policy_scd2",

    source="policy_changes_clean",

    # Composite business key
    keys=["member_id", "policy_id"],

    # Determines the ordering of incoming changes
    sequence_by=col("op_ts"),

    # Exclude CDC metadata from the target
    except_column_list=["op", "op_ts", "_rescued_data"],

    # Maintain historical versions
    stored_as_scd_type=2
)