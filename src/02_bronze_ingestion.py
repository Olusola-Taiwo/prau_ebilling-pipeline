from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
import time

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------

catalog = "prau_data_catalog"
schema = "bronze"
raw_base = "/Volumes/prau_data_catalog/raw/source_systems"

batch_id = str(uuid.uuid4())
ingest_time = current_timestamp()

sources = {
    "order_hdr": f"{raw_base}/order_hdr/",
    "customer_details": f"{raw_base}/customer_details/",
    "account": f"{raw_base}/account/",
    "ddp": f"{raw_base}/ddp/"
}

# -------------------------------------------------------------------
# Bronze ingestion function
# -------------------------------------------------------------------

def ingest_to_bronze(name, path):
    df = spark.read.json(path)

    bronze_df = df.withColumn("ingest_time", ingest_time) \
                  .withColumn("batch_id", lit(batch_id)) \
                  .withColumn("source_path", lit(path))

    table_name = f"{catalog}.{schema}.{name}_bronze"

    bronze_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

    print(f"Bronze ingestion completed → {table_name}")

# -------------------------------------------------------------------
# Execute ingestion for all raw sources
# -------------------------------------------------------------------

for name, path in sources.items():
    ingest_to_bronze(name, path)

print("All Bronze ingestion tasks completed.")
