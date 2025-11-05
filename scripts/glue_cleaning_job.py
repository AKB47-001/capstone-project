# ============================================================
# 🧼 AWS Glue Cleaning Job (Kaggle-Equivalent Version)
# ============================================================

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3", "PREPROCESSED_S3"])
RAW = args["RAW_S3"].rstrip("/")
PRE = args["PREPROCESSED_S3"].rstrip("/")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.sparkContext.setLogLevel("WARN")

def read_parquet(path):
    try:
        df = spark.read.parquet(path)
        print(f"📥 Loaded {path} ({df.count()} rows)")
        return df
    except Exception as e:
        print(f"⚠️ Could not read {path}: {e}")
        return None

def clean_texts(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
    return df.dropDuplicates()

print("🚀 Starting Data Cleaning Job...\n")

# customers
c = read_parquet(f"{RAW}/customers")
if c is not None:
    c = clean_texts(c).filter(F.col("customer_id").isNotNull())
    c.write.mode("overwrite").parquet(f"{PRE}/customers")
    print("✅ Cleaned customers")

# sellers
s = read_parquet(f"{RAW}/sellers")
if s is not None:
    s = clean_texts(s).filter(F.col("seller_id").isNotNull())
    s.write.mode("overwrite").parquet(f"{PRE}/sellers")
    print("✅ Cleaned sellers")

# orders
o = read_parquet(f"{RAW}/orders")
if o is not None:
    o = clean_texts(o).filter(F.col("order_id").isNotNull())
    o = o.filter(F.col("order_purchase_timestamp").isNotNull())
    o = o.filter(F.col("order_delivered_customer_date").isNotNull())
    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if col in o.columns:
            o = o.withColumn(col, F.to_timestamp(F.col(col)))
    o.write.mode("overwrite").parquet(f"{PRE}/orders")
    print("✅ Cleaned orders")

# order_items
oi = read_parquet(f"{RAW}/order_items")
if oi is not None:
    oi = clean_texts(oi).filter((F.col("price") > 0) & (F.col("freight_value") >= 0))
    oi.write.mode("overwrite").parquet(f"{PRE}/order_items")
    print("✅ Cleaned order_items")

# order_payments
p = read_parquet(f"{RAW}/order_payments")
if p is not None:
    p = clean_texts(p).filter(F.col("payment_value") >= 0)
    p.write.mode("overwrite").parquet(f"{PRE}/order_payments")
    print("✅ Cleaned order_payments")

# order_reviews
r = read_parquet(f"{RAW}/order_reviews")
if r is not None:
    r = clean_texts(r)
    r = r.filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
    for col in ["review_creation_date", "review_answer_timestamp"]:
        if col in r.columns:
            r = r.withColumn(col, F.to_timestamp(F.col(col)))
    r.write.mode("overwrite").parquet(f"{PRE}/order_reviews")
    print("✅ Cleaned order_reviews")

# products
pr = read_parquet(f"{RAW}/products")
if pr is not None:
    pr = clean_texts(pr)
    if all(c in pr.columns for c in ["product_weight_g","product_length_cm","product_height_cm","product_width_cm"]):
        pr = pr.filter(
            (F.col("product_weight_g") > 0) &
            (F.col("product_length_cm") > 0) &
            (F.col("product_height_cm") > 0) &
            (F.col("product_width_cm") > 0)
        )
    pr.write.mode("overwrite").parquet(f"{PRE}/products")
    print("✅ Cleaned products")

# geolocation
g = read_parquet(f"{RAW}/geolocation")
if g is not None:
    g = clean_texts(g)
    g = g.filter(
        (F.col("geolocation_lat").between(-90, 90)) &
        (F.col("geolocation_lng").between(-180, 180)) &
        F.col("geolocation_city").isNotNull()
    )
    g.write.mode("overwrite").parquet(f"{PRE}/geolocation")
    print("✅ Cleaned geolocation")

print("\n🎉 All tables cleaned successfully and saved to preprocessed layer.")
job.commit()
