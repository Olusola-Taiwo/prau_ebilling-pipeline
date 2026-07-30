from pyspark.sql.functions import *
from pyspark.sql.types import *

catalog = "prau_data_catalog"
silver_plus = f"{catalog}.silver_plus"
gold = f"{catalog}.gold"

df = spark.table(f"{silver_plus}.order_enriched_silver")

# ---------------------------------------------------------
# KPI 1: Delivery Mode Distribution
# ---------------------------------------------------------

delivery_kpi = (
    df.groupBy("final_delivery_mode")
      .agg(count("*").alias("total_invoices"))
)

delivery_kpi.write.mode("overwrite").format("delta").saveAsTable(
    f"{gold}.delivery_mode_kpi"
)

print("Gold KPI created → delivery_mode_kpi")

# ---------------------------------------------------------
# KPI 2: Billing Risk Distribution
# ---------------------------------------------------------

risk_kpi = (
    df.groupBy("billing_risk_flag")
      .agg(count("*").alias("customers"))
)

risk_kpi.write.mode("overwrite").format("delta").saveAsTable(
    f"{gold}.billing_risk_kpi"
)

print("Gold KPI created → billing_risk_kpi")

# ---------------------------------------------------------
# KPI 3: Customer Country Distribution
# ---------------------------------------------------------

country_kpi = (
    df.groupBy("country")
      .agg(count("*").alias("customer_count"))
)

country_kpi.write.mode("overwrite").format("delta").saveAsTable(
    f"{gold}.customer_country_kpi"
)

print("Gold KPI created → customer_country_kpi")

# ---------------------------------------------------------
# KPI 4: Invoice Volume by Billing Type
# ---------------------------------------------------------

invoice_kpi = (
    df.groupBy("billing_type")
      .agg(count("*").alias("invoice_count"))
)

invoice_kpi.write.mode("overwrite").format("delta").saveAsTable(
    f"{gold}.invoice_volume_kpi"
)

print("Gold KPI created → invoice_volume_kpi")

# ---------------------------------------------------------
print("All Gold KPI modelling tasks completed.")
