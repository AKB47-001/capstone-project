# ============================================================
# 🧩 AWS Glue Normalization Job
# ============================================================
# Purpose:
# 1️⃣ Read denormalized dataset (CSV or Excel exported as CSV) from S3
# 2️⃣ Split into normalized relational tables:
#     customers, sellers, orders, order_items, payments, reviews, products, geolocation
# 3️⃣ Write each table to Parquet under raw/normalized/
# 4️⃣ Print sample records, record counts, and stage logs for debugging
# ============================================================

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ------------------------------------------------------------------
# Read arguments from Glue Job parameters
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "source_format", "output_s3", "aws_region"])
source_path = args["source_path"]
source_format = args["source_format"].lower()
output_s3 = args["output_s3"]

# ------------------------------------------------------------------
# Initialize Spark / Glue context
# ------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.sparkContext.setLogLevel("WARN")

print("\n🚀 Starting Normalization Job")
print(f"📂 Source Path: {source_path}")
print(f"📦 Output Folder: {output_s3}")
print("============================================================")

# ------------------------------------------------------------------
# Step 1️⃣ — Read the Denormalized Dataset
# ------------------------------------------------------------------
if source_format != "csv":
    raise Exception("Only CSV format is supported in this job.")

print("\n📥 Reading denormalized CSV data from S3...")
df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .option("quote", "\"")
    .option("sep", ",")
    .csv(source_path)
)

record_count = df.count()
print(f"✅ Successfully read {record_count} records from {source_path}")
print("📊 Sample of Denormalized Data:")
df.show(5, truncate=False)

# ------------------------------------------------------------------
# Step 2️⃣ — Define the Normalized Table Structures
# ------------------------------------------------------------------
print("\n🧩 Defining normalization schemas...")

TABLES = {
    "customers": ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"],
    "sellers": ["seller_id","seller_zip_code_prefix","seller_city","seller_state"],
    "orders": ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
               "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],
    "order_items": ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"],
    "order_payments": ["order_id","payment_sequential","payment_type","payment_installments","payment_value"],
    "order_reviews": ["review_id","order_id","review_score","review_comment_title","review_comment_message",
                      "review_creation_date","review_answer_timestamp"],
    "products": ["product_id","product_category_name","product_name_length","product_description_length",
                 "product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"],
    "geolocation": ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"],
    "product_category_name_translation": ["product_category_name","product_category_name_english"]
}

# ------------------------------------------------------------------
# Step 3️⃣ — Utility function to select and validate columns
# ------------------------------------------------------------------
def select_cols(df, cols):
    present = [c for c in cols if c in df.columns]
    if not present:
        print(f"⚠️ Skipping table — no matching columns: {cols}")
        return None
    return df.select(*present).dropDuplicates()

# ------------------------------------------------------------------
# Step 4️⃣ — Normalize Each Table and Write to S3
# ------------------------------------------------------------------
for name, cols in TABLES.items():
    print("\n============================================================")
    print(f"🔹 Normalizing Table: {name}")
    print("============================================================")

    # Extract subset of columns
    sub = select_cols(df, cols)
    if sub is None:
        continue

    # Convert timestamp/date columns properly
    for c in [x for x in sub.columns if "timestamp" in x or "date" in x]:
        sub = sub.withColumn(c, F.to_timestamp(F.col(c)))

    # Add partition columns for orders table
    if name == "orders" and "order_purchase_timestamp" in sub.columns:
        sub = sub.withColumn("order_year", F.year("order_purchase_timestamp")).withColumn(
            "order_month", F.month("order_purchase_timestamp")
        )

    # Count records
    count = sub.count()
    print(f"✅ Table '{name}' normalized successfully with {count} records.")

    # Show sample records
    print(f"📊 Sample records from '{name}':")
    sub.show(5, truncate=False)

    # Write to S3
    out_path = f"{output_s3}/{name}"
    print(f"💾 Writing table '{name}' to {out_path} ...")
    if name == "orders" and "order_year" in sub.columns:
        sub.write.mode("overwrite").partitionBy("order_year","order_month").parquet(out_path)
    else:
        sub.coalesce(1).write.mode("overwrite").parquet(out_path)

    print(f"✅ Table '{name}' written successfully to S3 at {out_path}")

print("\n🎉 Normalization complete! All tables written to:")
print(f"➡️ {output_s3}")
print("============================================================")

job.commit()
