from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = "prau_data_catalog"
silver = f"{catalog}.silver"
silver_plus = f"{catalog}.silver_plus"

# -------------------------------------------------------------------
# Ensure catalog + schemas exist
# -------------------------------------------------------------------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_plus}")

# -------------------------------------------------------------------
# Load Silver tables (rename ambiguous columns)
# -------------------------------------------------------------------

order_hdr = (
    spark.table(f"{silver}.order_hdr_silver")
    .withColumnRenamed("is_valid", "is_valid_order")
    .withColumnRenamed("batch_id", "batch_id_order")
    .withColumnRenamed("ingest_time", "ingest_time_order")
    .withColumnRenamed("source_path", "source_path_order")
)

customer = (
    spark.table(f"{silver}.customer_details_silver")
    .withColumnRenamed("is_valid", "is_valid_customer")
    .withColumnRenamed("batch_id", "batch_id_customer")
    .withColumnRenamed("ingest_time", "ingest_time_customer")
    .withColumnRenamed("source_path", "source_path_customer")
)

account = (
    spark.table(f"{silver}.account_silver")
    .withColumnRenamed("is_valid", "is_valid_account")
    .withColumnRenamed("batch_id", "batch_id_account")
    .withColumnRenamed("ingest_time", "ingest_time_account")
    .withColumnRenamed("source_path", "source_path_account")
)

ddp = (
    spark.table(f"{silver}.ddp_silver")
    .withColumnRenamed("is_valid", "is_valid_ddp")
    .withColumnRenamed("batch_id", "batch_id_ddp")
    .withColumnRenamed("ingest_time", "ingest_time_ddp")
    .withColumnRenamed("source_path", "source_path_ddp")
)

# -------------------------------------------------------------------
# Join all Silver datasets into a unified Silver+ table
# -------------------------------------------------------------------

joined = (
    order_hdr.alias("o")
    .join(customer.alias("c"), "customer_id", "left")
    .join(account.alias("a"), "customer_id", "left")
    .join(ddp.alias("d"), "customer_id", "left")
)

# -------------------------------------------------------------------
# Apply business rules
# -------------------------------------------------------------------

joined = joined.withColumn(
    "final_delivery_mode",
    when(col("delivery_mode_preference") == "EMAIL", "EMAIL")
    .when(col("delivery_mode_preference") == "PRINT", "PRINT")
    .when(col("delivery_mode_preference") == "BOTH", "EMAIL_AND_PRINT")
    .when(col("delivery_mode_preference") == "IGNORE", "IGNORE")
    .otherwise("UNKNOWN")
)

joined = joined.withColumn(
    "billing_risk_flag",
    when(col("balance") < 0, "HIGH_RISK").otherwise("NORMAL")
)

joined = joined.withColumn(
    "is_valid_silver_plus",
    col("is_valid_order") &
    col("customer_id").isNotNull() &
    col("final_delivery_mode").isNotNull()
)

# -------------------------------------------------------------------
# Write Silver+ table
# -------------------------------------------------------------------

joined.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver_plus}.order_enriched_silver"
)

print("Silver+ business rules applied → order_enriched_silver")
