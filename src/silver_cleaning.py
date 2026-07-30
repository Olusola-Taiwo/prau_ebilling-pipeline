from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = "prau_data_catalog"
bronze = f"{catalog}.bronze"
silver = f"{catalog}.silver"

# -------------------------------------------------------------------
# Helper: Standard DQ flag
# -------------------------------------------------------------------

def add_dq_flags(df, required_cols):
    return df.withColumn(
        "is_valid",
        expr(" AND ".join([f"{c} IS NOT NULL" for c in required_cols]))
    )

# -------------------------------------------------------------------
# ORDER_HDR Silver
# -------------------------------------------------------------------

order_hdr = spark.table(f"{bronze}.order_hdr_bronze")

order_hdr_silver = (
    order_hdr
    .withColumn("invoice_date", to_date("invoice_date"))
    .withColumn("due_date", to_date("due_date"))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("billing_type", upper(col("billing_type")))
    .withColumn("currency", upper(col("currency")))
)

order_hdr_silver = add_dq_flags(
    order_hdr_silver,
    ["order_id", "customer_id", "invoice_date", "amount"]
)

order_hdr_silver.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver}.order_hdr_silver"
)

print("Silver cleaned → order_hdr_silver")

# -------------------------------------------------------------------
# CUSTOMER_DETAILS Silver
# -------------------------------------------------------------------

customer = spark.table(f"{bronze}.customer_details_bronze")

customer_silver = (
    customer
    .withColumn("customer_name", initcap("customer_name"))
    .withColumn("country", upper("country"))
)

customer_silver = add_dq_flags(
    customer_silver,
    ["customer_id", "customer_name"]
)

customer_silver.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver}.customer_details_silver"
)

print("Silver cleaned → customer_details_silver")

# -------------------------------------------------------------------
# ACCOUNT Silver
# -------------------------------------------------------------------

account = spark.table(f"{bronze}.account_bronze")

account_silver = (
    account
    .withColumn("account_type", upper("account_type"))
    .withColumn("balance", col("balance").cast("double"))
)

account_silver = add_dq_flags(
    account_silver,
    ["customer_id", "account_id"]
)

account_silver.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver}.account_silver"
)

print("Silver cleaned → account_silver")

# -------------------------------------------------------------------
# DDP Silver
# -------------------------------------------------------------------

ddp = spark.table(f"{bronze}.ddp_bronze")

ddp_silver = (
    ddp
    .withColumn("delivery_mode_preference", upper("delivery_mode_preference"))
    .withColumn("last_updated", to_timestamp("last_updated"))
)

ddp_silver = add_dq_flags(
    ddp_silver,
    ["customer_id", "delivery_mode_preference"]
)

ddp_silver.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver}.ddp_silver"
)

print("Silver cleaned → ddp_silver")

# -------------------------------------------------------------------
print("All Silver cleaning tasks completed.")
