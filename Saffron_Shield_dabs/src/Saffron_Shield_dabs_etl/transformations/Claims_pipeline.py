from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")

# --------------------------- Auto-loader pipeline claims ingestion ----------------------------

@dp.table(name=f"{catalog}.bronze.claims")
def claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/claims/raw/landing/claims/")
        .select(
            col("*"),
            col("_metadata.file_name").alias("_source_file"),
            current_timestamp().alias("_ingest_ts")
        )
    )

# --------------------------- Silver claims Cleaning -----------------------------

# ============================================================
# 1. TEMPORARY VIEW: STANDARDIZATION + DATA QUALITY
# ============================================================

@dp.materialized_view(name="claims_clean_view")
def claims_clean_view():

    claims_df = spark.read.table(f"{catalog}.bronze.claims")

    standardized_claims_df = (
        claims_df
        .dropna(how="all")
        .drop("_source_file", "_ingest_ts", "_rescued_data")

        .withColumns({

            # String standardization
            "claim_id": trim(col("claim_id")),
            "claim_type": trim(col("claim_type")),
            "channel": trim(col("channel")),
            "diagnosis_code": trim(col("diagnosis_code")),
            "hospital_id": trim(col("hospital_id")),
            "policy_id": trim(col("policy_id")),
            "member_id": trim(col("member_id")),
            "pre_auth_id": trim(col("pre_auth_id")),
            "procedure_code": trim(col("procedure_code")),
            "rejection_code": trim(col("rejection_code")),
            "status": lower(trim(col("status"))),

            # Date standardization
            "admission_date": coalesce(
                try_to_date(col("admission_date"), "yyyy-MM-dd"),
                try_to_date(col("admission_date"), "MM/dd/yyyy"),
                try_to_date(col("admission_date"), "yyyy/MM/dd"),
                try_to_date(col("admission_date"), "MM-dd-yyyy"),
                try_to_date(col("admission_date"), "MMM d, yyyy"),
                try_to_date(col("admission_date"), "dd-MM-yy")
            ),

            "discharge_date": coalesce(
                try_to_date(col("discharge_date"), "yyyy-MM-dd"),
                try_to_date(col("discharge_date"), "MM/dd/yyyy"),
                try_to_date(col("discharge_date"), "yyyy/MM/dd"),
                try_to_date(col("discharge_date"), "MM-dd-yyyy"),
                try_to_date(col("discharge_date"), "MMM d, yyyy"),
                try_to_date(col("discharge_date"), "dd-MM-yy")
            ),

            # Timestamp standardization
            "settled_ts": coalesce(
                try_to_timestamp(col("settled_ts"), lit("yyyy-MM-dd'T'HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("MM/dd/yyyy HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("yyyy/MM/dd HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("MM-dd-yyyy HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("MMM d, yyyy HH:mm:ss")),
                try_to_timestamp(col("settled_ts"), lit("dd-MM-yy HH:mm:ss"))
            ),

            "submitted_ts": coalesce(
                try_to_timestamp(col("submitted_ts"), lit("yyyy-MM-dd'T'HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("MM/dd/yyyy HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("yyyy/MM/dd HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("MM-dd-yyyy HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("MMM d, yyyy HH:mm:ss")),
                try_to_timestamp(col("submitted_ts"), lit("dd-MM-yy HH:mm:ss"))
            )
        })
    )

    # ========================================================
    # 2. Q-A: REFERENTIAL INTEGRITY
    # ========================================================

    valid_hospital_df = (
        spark.read.table(f"{catalog}.raw.hospitals")
        .select(col("hospital_id").alias("_valid_hospital_id"))
        .distinct()
        .withColumn("_hospital_exists", lit(True))
    )

    valid_member_df = (
        spark.read.table(f"{catalog}.raw.members")
        .select(col("member_id").alias("_valid_member_id"))
        .distinct()
        .withColumn("_member_exists", lit(True))
    )

    standardized_claims_df = (
        standardized_claims_df
        .join(
            valid_hospital_df,
            standardized_claims_df.hospital_id == valid_hospital_df._valid_hospital_id,
            "left"
        )
        .join(
            valid_member_df,
            standardized_claims_df.member_id == valid_member_df._valid_member_id,
            "left"
        )
    )

    # ========================================================
    # 3. Q-A TO Q-D: VALIDATION FLAGS
    # ========================================================

    standardized_claims_df = standardized_claims_df.withColumns({

        # Q-A: Referential integrity
        "_q_a_valid": coalesce(
            col("_hospital_exists").isNotNull()
            & col("_member_exists").isNotNull(),
            lit(False)
        ),

        # Q-B: Date ordering
        "_q_b_valid": coalesce(
            col("admission_date").isNotNull()
            & col("discharge_date").isNotNull()
            & (col("admission_date") <= col("discharge_date"))
            & (
                (col("status") != "settled")
                | (
                    col("submitted_ts").isNotNull()
                    & col("settled_ts").isNotNull()
                    & (col("settled_ts") >= col("submitted_ts"))
                )
            ),
            lit(False)
        ),

        # Q-C: Amount sanity
        "_q_c_valid": coalesce(
            col("billed_amount").isNotNull()
            & (col("billed_amount") > 0)
            & (
                col("approved_amount").isNull()
                | (col("approved_amount") >= 0)
            )
            & (
                col("approved_amount").isNull()
                | (col("approved_amount") <= col("billed_amount"))
            ),
            lit(False)
        ),

        # Q-D: Status consistency
        "_q_d_valid": coalesce(

            col("status").isin(
                "submitted", "approved", "rejected", "settled"
            )

            # Submitted status
            & (
                (col("status") != "submitted")
                | (
                    col("approved_amount").isNull()
                    & col("rejection_code").isNull()
                    & col("settled_ts").isNull()
                )
            )

            # Approved status
            & (
                (col("status") != "approved")
                | (
                    col("approved_amount").isNotNull()
                    & col("rejection_code").isNull()
                    & col("settled_ts").isNull()
                )
            )

            # Rejected status
            & (
                (col("status") != "rejected")
                | (
                    col("rejection_code").isNotNull()
                    & col("settled_ts").isNull()
                )
            )

            # Settled status
            & (
                (col("status") != "settled")
                | (
                    col("approved_amount").isNotNull()
                    & col("settled_ts").isNotNull()
                    & col("rejection_code").isNull()
                )
            ),

            lit(False)
        )
    })

    # ========================================================
    # Q-E: ROOM MATH VALIDATION
    # ========================================================

    standardized_claims_df = standardized_claims_df.withColumn(
        "_calculated_room_days",
        when(
            datediff(
                col("discharge_date"),
                col("admission_date")
            ) == 0,
            lit(1)
        ).otherwise(
            datediff(
                col("discharge_date"),
                col("admission_date")
            )
        )
    )

    standardized_claims_df = standardized_claims_df.withColumn(
        "_q_e_valid",
        coalesce(
            col("room_days").isNotNull()
            & col("_calculated_room_days").isNotNull()
            & (col("room_days") == col("_calculated_room_days")),
            lit(False)
        )
    )

    # ============================================================
    # Q-F: DUPLICATE CLAIM ID VALIDATION
    # ============================================================

    # Rank records based on claim_id and latest submitted_ts
    claim_window = (
        Window
        .partitionBy("claim_id")
        .orderBy(
            col("submitted_ts").desc_nulls_last()
        )
    )

    standardized_claims_df = (
        standardized_claims_df
        .withColumn(
            "_claim_rank",
            row_number().over(claim_window)
        )
        .withColumn(
            "_q_f_valid",
            coalesce(
                col("claim_id").isNotNull()
                & (col("_claim_rank") == 1),
                lit(False)
            )
        )
    )

    # ========================================================
    # 5. QUARANTINE REASONS
    # ========================================================

    standardized_claims_df = standardized_claims_df.withColumn(
        "_quarantine_reason",
        nullif(
            concat_ws(
                ", ",

                when(
                    ~col("_q_a_valid"),
                    lit("Q-A: Invalid hospital_id or member_id reference")
                ),

                when(
                    ~col("_q_b_valid"),
                    lit("Q-B: Invalid date ordering or settled timestamp")
                ),

                when(
                    ~col("_q_c_valid"),
                    lit("Q-C: Invalid billed_amount or approved_amount")
                ),

                when(
                    ~col("_q_d_valid"),
                    lit("Q-D: Invalid claim status or status consistency")
                ),

                when(
                    ~col("_q_e_valid"),
                    lit("Q-E: Invalid room_days calculation")
                ),

                when(
                    col("claim_id").isNull(),
                    lit("Q-F: claim_id is NULL")
                ),

                when(
                    col("claim_id").isNotNull()
                    & (col("_claim_rank") > 1),
                    lit("Q-F: Duplicate claim_id - older record")
                )
            ),
            lit("")
        )
    )

    # Remove internal join columns
    return standardized_claims_df.drop(
        "_valid_hospital_id",
        "_valid_member_id",
        "_hospital_exists",
        "_member_exists"
    )


# ============================================================
# 6. CLEAN CLAIMS TABLE
# ============================================================

@dp.table(name=f"{catalog}.silver.claims_clean")
def claims_clean():

    return (
        spark.read.table("claims_clean_view")
        .filter(col("_quarantine_reason").isNull())
        .drop(
            "_q_a_valid",
            "_q_b_valid",
            "_q_c_valid",
            "_q_d_valid",
            "_q_e_valid",
            "_q_f_valid",
            "_claim_rank",
            "_quarantine_reason",
            "_ingest_ts"
        )
    )

# ============================================================
# 7. QUARANTINE CLAIMS TABLE
# ============================================================

@dp.table(name=f"{catalog}.silver.claims_quarantine")
def claims_quarantine():

    return (
        spark.read.table("claims_clean_view")
        .filter(col("_quarantine_reason").isNotNull())
        .drop(
            "_q_a_valid",
            "_q_b_valid",
            "_q_c_valid",
            "_q_d_valid",
            "_q_e_valid",
            "_q_f_valid",
            "_ingest_ts"
        )
    )


# ----------------------------- gold tables -----------------------------
@dp.materialized_view(
    name=f"{catalog}.gold.claims_enriched",
    comment="Enriched claims with policy, plan, hospital details and business metrics",
    cluster_by=["admission_date", "hospital_id"]
)
def claims_enriched():

    # --------------------------------------------------
    # 1. Read Silver Claims
    # --------------------------------------------------
    claims_df = (
        spark.read.table(f"{catalog}.silver.claims_clean")
        .alias("c")
    )

    # --------------------------------------------------
    # 2. Read Policy SCD Type 2 History
    # --------------------------------------------------
    policy_df = (
        spark.read.table(f"{catalog}.silver.policy_scd2")
        .alias("p")
    )

    # --------------------------------------------------
    # 3. Read Plan Reference Data
    # --------------------------------------------------
    plans_df = (
        spark.read.table(f"{catalog}.raw.plans")
        .alias("pl")
    )

    # --------------------------------------------------
    # 4. Read Hospital Reference Data
    # --------------------------------------------------
    hospitals_df = (
        spark.read.table(f"{catalog}.raw.hospitals")
        .alias("h")
    )

    # --------------------------------------------------
    # 5. Read Hospital Network History
    # --------------------------------------------------
    network_history_df = (
        spark.read.table("claims.raw.hospital_network_history")
        .alias("hn")
    )

    # --------------------------------------------------
    # 6. Join Claims with Policy SCD2
    #    Identify the policy version valid at admission
    # --------------------------------------------------
    claims_policy_df = claims_df.join(
        policy_df,
        (
            (col("c.member_id") == col("p.member_id"))
            & (col("c.policy_id") == col("p.policy_id"))
            & (
                col("c.admission_date")
                >= to_date(col("p.__START_AT"))
            )
            & (
                col("p.__END_AT").isNull()
                | (
                    col("c.admission_date")
                    < to_date(col("p.__END_AT"))
                )
            )
        ),
        "left"
    )

    # --------------------------------------------------
    # 7. Join Policy with Plan Details
    # --------------------------------------------------
    claims_plan_df = claims_policy_df.join(
        plans_df,
        col("p.plan_code") == col("pl.plan_code"),
        "left"
    )

    # --------------------------------------------------
    # 8. Join Claims with Hospital Details
    # --------------------------------------------------
    claims_hospital_df = claims_plan_df.join(
        hospitals_df,
        col("c.hospital_id") == col("h.hospital_id"),
        "left"
    )

    # --------------------------------------------------
    # 9. Join Hospital Network History
    #    Check network membership at admission date
    # --------------------------------------------------
    enriched_df = claims_hospital_df.join(
        network_history_df,
        (
            (col("c.hospital_id") == col("hn.hospital_id"))
            & (
                col("c.admission_date")
                >= to_date(col("hn.network_from"))
            )
            & (
                col("hn.network_to").isNull()
                | (
                    col("c.admission_date")
                    < to_date(col("hn.network_to"))
                )
            )
        ),
        "left"
    )

    # --------------------------------------------------
    # 10. Select Original Fields + Enrichment + Metrics
    # --------------------------------------------------
    result_df = enriched_df.select(

        # Original Claim Fields
        col("c.claim_id"),
        col("c.member_id"),
        col("c.policy_id"),
        col("c.hospital_id"),
        col("c.admission_date"),
        col("c.discharge_date"),
        col("c.diagnosis_code"),
        col("c.procedure_code"),
        col("c.is_pre_existing"),
        col("c.claim_type"),
        col("c.billed_amount"),
        col("c.room_rent_per_day"),
        col("c.room_days"),
        col("c.status"),
        col("c.approved_amount"),
        col("c.rejection_code"),
        col("c.submitted_ts"),
        col("c.settled_ts"),
        col("c.channel"),

        # Policy and Plan Enrichment
        col("p.plan_code").alias("plan_code_at_admission"),
        col("pl.plan_family"),
        to_date(col("p.effective_from")).alias("policy_start_date"),
        col("pl.room_rent_cap_per_day"),

        # Hospital Enrichment
        col("h.city").alias("hospital_city"),
        col("h.city_tier").alias("hospital_city_tier"),

        # Historical Network Status
        coalesce(
            col("hn.hospital_id").isNotNull(),
            lit(False)
        ).alias("is_network_at_admission"),

        # --------------------------------------------------
        # Derived Business Metrics
        # --------------------------------------------------

        # Admission Month
        trunc(
            col("c.admission_date"),
            "month"
        ).cast("date").alias("admission_month"),

        # Policy Tenure in Months at Admission
        floor(
            months_between(
                col("c.admission_date"),
                to_date(col("p.effective_from"))
            )
        ).cast("int").alias("policy_tenure_months"),

        # Length of Stay
        datediff(
            col("c.discharge_date"),
            col("c.admission_date")
        ).alias("length_of_stay"),

        # Turnaround Days (only when settled)
        when(
            col("c.status") == "settled",
            datediff(
                to_date(col("c.settled_ts")),
                to_date(col("c.submitted_ts"))
            )
        ).otherwise(
            lit(None).cast("int")
        ).alias("turnaround_days"),

        # Room Rent Over Cap
        when(
            col("pl.room_rent_cap_per_day").isNull()
            | col("c.room_rent_per_day").isNull(),
            lit(None).cast("boolean")
        ).otherwise(
            col("c.room_rent_per_day")
            > col("pl.room_rent_cap_per_day")
        ).alias("room_rent_over_cap")
    )

    return result_df

# ----------------------- hospital_monthly -------------------------

@dp.materialized_view(
    name=f"{catalog}.gold.hospital_monthly",
    comment="Monthly hospital-level claims and financial summary"
)
def hospital_monthly():

    # 1. Read enriched claims from Gold
    claims_df = spark.read.table(
        f"{catalog}.gold.claims_enriched"
    )

    # 2. Aggregate claims by hospital and admission month
    hospital_monthly_df = (
        claims_df
        .groupBy(
            "hospital_id",
            "admission_month"
        )
        .agg(

            # Total number of distinct claims
            countDistinct("claim_id")
                .cast("int")
                .alias("claim_count"),

            # Total billed amount
            sum("billed_amount")
                .cast("decimal(18,2)")
                .alias("total_billed"),

            # Total approved amount (0 when no amount is available)
            coalesce(
                sum("approved_amount"),
                lit(0).cast("decimal(18,2)")
            )
            .cast("decimal(18,2)")
            .alias("total_approved"),

            # Count of rejected claims
            sum(
                when(col("status") == "rejected", 1)
                .otherwise(0)
            )
            .cast("int")
            .alias("rejected_count")
        )
    )

    return hospital_monthly_df