from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from datetime import datetime

catalog = "prau_data_catalog"
bronze = f"{catalog}.bronze"
silver = f"{catalog}.silver"
silver_plus = f"{catalog}.silver_plus"
gold = f"{catalog}.gold"

batch_id = str(uuid.uuid4())
run_time = datetime.utcnow()   # FIX: Python timestamp, not Spark Column

# ---------------------------------------------------------
# Count records in each layer
# ---------------------------------------------------------

counts = {
    "order_hdr_bronze": spark.table(f"{bronze}.order_hdr_bronze").count(),
    "customer_details_bronze": spark.table(f"{bronze}.customer_details_bronze").count(),
    "account_bronze": spark.table(f"{bronze}.account_bronze").count(),
    "ddp_bronze": spark.table(f"{bronze}.ddp_bronze").count(),

    "order_hdr_silver": spark.table(f"{silver}.order_hdr_silver").count(),
    "customer_details_silver": spark.table(f"{silver}.customer_details_silver").count(),
    "account_silver": spark.table(f"{silver}.account_silver").count(),
    "ddp_silver": spark.table(f"{silver}.ddp_silver").count(),

    "order_enriched_silver": spark.table(f"{silver_plus}.order_enriched_silver").count(),

    "delivery_mode_kpi": spark.table(f"{gold}.delivery_mode_kpi").count(),
    "billing_risk_kpi": spark.table(f"{gold}.billing_risk_kpi").count(),
    "customer_country_kpi": spark.table(f"{gold}.customer_country_kpi").count(),
    "invoice_volume_kpi": spark.table(f"{gold}.invoice_volume_kpi").count()
}

# ---------------------------------------------------------
# Convert to DataFrame with explicit schema
# ---------------------------------------------------------

schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("run_time", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("record_count", LongType(), False),
    StructField("status", StringType(), False)
])

audit_rows = [
    (batch_id, run_time, layer, count, "SUCCESS")
    for layer, count in counts.items()
]

audit_df = spark.createDataFrame(audit_rows, schema)

# ---------------------------------------------------------
# Write audit log
# ---------------------------------------------------------

audit_df.write.mode("append").format("delta").saveAsTable(
    f"{gold}.audit_log"
)

print("Audit logging completed → audit_log")
