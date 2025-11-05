# ============================================================
# 🧩 AWS Glue Normalization Job (No Partitioning)
# Converts a denormalized CSV into normalized Parquet tables
# ============================================================

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "source_format", "output_s3", "aws_region"]
)

source_path  = args["source_path"]
source_fmt   = args["source_format"].lower()
output_s3    = args["output_s3"].rstrip("/")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"🚀 Reading denormalized file from: {source_path}")
reader = spark.read.option("header", "true").option("multiLine", "true").option("escape", "\"")

if source_fmt == "csv":
    df = reader.csv(source_path)
else:
    raise ValueError("Only CSV input is supported in this job configuration.")

print(f"✅ Loaded rows: {df.count()}")
df.show(5, truncate=False)

# ---------------------------------------------
# Define normalized tables (no partitioning)
# ---------------------------------------------
TABLES = {
    "customers": [
        "customer_id","customer_unique_id","customer_zip_code_prefix",
        "customer_city","customer_state"
    ],
    "sellers": [
        "seller_id","seller_zip_code_prefix","seller_city","seller_state"
    ],
    "orders": [
        "order_id","customer_id","order_status",
        "order_purchase_timestamp","order_approved_at",
        "order_delivered_carrier_date","order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "order_items": [
        "order_id","order_item_id","product_id","seller_id",
        "shipping_limit_date","price","freight_value"
    ],
    "order_payments": [
        "order_id","payment_sequential","payment_type",
        "payment_installments","payment_value"
    ],
    "order_reviews": [
        "review_id","order_id","review_score",
        "review_comment_title","review_comment_message",
        "review_creation_date","review_answer_timestamp"
    ],
    "products": [
        "product_id","product_category_name","product_name_length",
        "product_description_length","product_photos_qty",
        "product_weight_g","product_length_cm","product_height_cm",
        "product_width_cm"
    ],
    "geolocation": [
        "geolocation_zip_code_prefix","geolocation_lat","geolocation_lng",
        "geolocation_city","geolocation_state"
    ],
    "product_category_name_translation": [
        "product_category_name","product_category_name_english"
    ]

}

# write each table without any partitioning
for name, cols in TABLES.items():
    present = [c for c in cols if c in df.columns]
    if not present:
        print(f"⚠️ Skipping {name}: none of the expected columns were found")
        continue

    sub = df.select(*present).dropDuplicates()

    # convert likely date/time columns to timestamp
    for c in sub.columns:
        if any(k in c for k in ["timestamp", "date"]):
            sub = sub.withColumn(c, F.to_timestamp(F.col(c)))

    dest = f"{output_s3}/{name}"
    print(f"💾 Writing {name} → {dest} (no partitioning)")
    sub.write.mode("overwrite").parquet(dest)

print("🎉 Normalization finished (no partition columns created).")
job.commit()
