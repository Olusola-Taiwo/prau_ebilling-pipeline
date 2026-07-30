from pyspark.sql import Row
from pyspark.sql.types import *
import uuid
import random
from datetime import datetime, timedelta

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------

def random_date():
    return (datetime.now() - timedelta(days=random.randint(0, 60))).date()

# -------------------------------------------------------------------
# Schemas
# -------------------------------------------------------------------

order_hdr_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("due_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("billing_type", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("status", StringType(), True)
])

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True)
])

account_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True)
])

ddp_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("delivery_mode_preference", StringType(), True),
    StructField("last_updated", StringType(), True)
])

# -------------------------------------------------------------------
# Data Generators (using Row objects)
# -------------------------------------------------------------------

def generate_order_hdr(n=50000):
    billing_types = ["MONTHLY", "ADHOC", "USAGE", "SUBSCRIPTION"]
    currencies = ["GBP", "USD", "EUR", "NGN"]
    source_systems = ["ERP", "CRM", "OMS", "FINANCE"]

    rows = []
    for _ in range(n):
        rows.append(Row(
            order_id=str(uuid.uuid4()),
            customer_id=random.randint(100000, 999999),
            invoice_date=str(random_date()),
            due_date=str(random_date()),
            amount=float(round(random.uniform(10, 5000), 2)),
            currency=random.choice(currencies),
            billing_type=random.choice(billing_types),
            source_system=random.choice(source_systems),
            status=random.choice(["GENERATED", "PENDING", "FAILED"])
        ))
    return spark.createDataFrame(rows, schema=order_hdr_schema)


def generate_customer_details(n=20000):
    countries = ["GB", "US", "NG", "IN", "PK", "CN"]
    rows = []
    for _ in range(n):
        cid = random.randint(100000, 999999)
        rows.append(Row(
            customer_id=cid,
            customer_name=f"Customer_{cid}",
            country=random.choice(countries),
            email=f"customer{cid}@example.com",
            phone=f"+44{random.randint(7000000000, 7999999999)}"
        ))
    return spark.createDataFrame(rows, schema=customer_schema)


def generate_account(n=20000):
    account_types = ["CREDIT", "DEBIT", "DIRECT_DEBIT"]
    rows = []
    for _ in range(n):
        cid = random.randint(100000, 999999)
        rows.append(Row(
            customer_id=cid,
            account_id=str(uuid.uuid4()),
            account_type=random.choice(account_types),
            balance=float(round(random.uniform(-500, 5000), 2))
        ))
    return spark.createDataFrame(rows, schema=account_schema)


def generate_ddp(n=20000):
    delivery_modes = ["EMAIL", "PRINT", "BOTH", "IGNORE"]
    rows = []
    for _ in range(n):
        cid = random.randint(100000, 999999)
        rows.append(Row(
            customer_id=cid,
            delivery_mode_preference=random.choice(delivery_modes),
            last_updated=str(datetime.now())
        ))
    return spark.createDataFrame(rows, schema=ddp_schema)

# -------------------------------------------------------------------
# Generate all datasets
# -------------------------------------------------------------------

order_hdr_df = generate_order_hdr()
customer_df = generate_customer_details()
account_df = generate_account()
ddp_df = generate_ddp()

# -------------------------------------------------------------------
# Write to respective directories
# -------------------------------------------------------------------

base = "/Volumes/prau_data_catalog/raw/source_systems"

order_hdr_df.write.mode("overwrite").json(f"{base}/order_hdr/")
customer_df.write.mode("overwrite").json(f"{base}/customer_details/")
account_df.write.mode("overwrite").json(f"{base}/account/")
ddp_df.write.mode("overwrite").json(f"{base}/ddp/")

print("Raw data generation completed successfully")
