# ============================================================
# 🧼 Glue Cleaning Job — "Clean the Data" (table-aware)
# Mirrors the common cleaning rules used in popular Olist EDA
# notebooks: trim, drop duplicates, enforce valid IDs/status,
# handle timestamps, filter invalid/out-of-range values,
# and produce analysis-ready "preprocessed/" outputs.
# ============================================================

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ---------- args ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_S3", "PREPROCESSED_S3"])
RAW = args["RAW_S3"].rstrip("/")
PRE = args["PREPROCESSED_S3"].rstrip("/")

# ---------- spark ----------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.sparkContext.setLogLevel("WARN")

print("\n🚀 Starting Glue Cleaning Job")
print(f"📥 Normalized input: {RAW}")
print(f"📦 Preprocessed out: {PRE}\n")

# ---------- helpers ----------
def read_parquet(path: str):
    try:
        df = spark.read.parquet(path)
        print(f"✅ Read: {path}")
        print("👀 sample (raw):")
        df.show(5, truncate=False)
        print(f"🔢 count (raw): {df.count()}")
        return df
    except Exception as e:
        print(f"❌ Skip {path} (read error): {e}")
        return None

def to_timestamp_safely(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
            # Null out ancient/bad dates to avoid Spark 3.x parquet rebase errors
            df = df.withColumn(c, F.when(F.year(F.col(c)) < 1900, None).otherwise(F.col(c)))
    return df

def base_clean(df):
    # Trim/normalize all strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
    # Drop exact duplicates
    df = df.dropDuplicates()
    return df

def preview(title, df):
    print(f"\n📊 {title}")
    df.show(5, truncate=False)
    print(f"🔢 {title} count: {df.count()}\n")

def write_out(df, out, partitions=None):
    writer = df.coalesce(1).write.mode("overwrite").format("parquet")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(out)
    print(f"💾 wrote: {out}")

# ---------- table: customers ----------
def clean_customers():
    name = "customers"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    # Keep valid IDs
    df = df.filter(F.col("customer_id").isNotNull())
    # Optional: validate state code length (2), zip prefix present
    if "customer_state" in df.columns:
        df = df.filter((F.length("customer_state") == 2) | F.col("customer_state").isNull())
    if "customer_zip_code_prefix" in df.columns:
        df = df.filter(F.col("customer_zip_code_prefix").isNotNull())

    preview("customers (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: sellers ----------
def clean_sellers():
    name = "sellers"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    df = df.filter(F.col("seller_id").isNotNull())
    if "seller_state" in df.columns:
        df = df.filter((F.length("seller_state") == 2) | F.col("seller_state").isNull())
    if "seller_zip_code_prefix" in df.columns:
        df = df.filter(F.col("seller_zip_code_prefix").isNotNull())

    preview("sellers (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: products ----------
def clean_products():
    name = "products"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    df = df.filter(F.col("product_id").isNotNull())

    # Non-negative numeric attributes (drop obviously bad)
    numeric_cols = [
        "product_name_length","product_description_length",
        "product_photos_qty","product_weight_g",
        "product_length_cm","product_height_cm","product_width_cm",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.filter((F.col(c) >= 0) | F.col(c).isNull())

    preview("products (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: geolocation ----------
def clean_geolocation():
    name = "geolocation"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    # Valid coordinate bounds
    if "geolocation_lat" in df.columns:
        df = df.filter((F.col("geolocation_lat") <= 90) & (F.col("geolocation_lat") >= -90))
    if "geolocation_lng" in df.columns:
        df = df.filter((F.col("geolocation_lng") <= 180) & (F.col("geolocation_lng") >= -180))
    if "geolocation_zip_code_prefix" in df.columns:
        df = df.filter(F.col("geolocation_zip_code_prefix").isNotNull())

    preview("geolocation (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: order_items ----------
def clean_order_items():
    name = "order_items"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    # Required IDs
    df = df.filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
    # Non-negative price/freight
    for c in ("price", "freight_value"):
        if c in df.columns:
            df = df.filter(F.col(c).isNotNull() & (F.col(c) >= 0))
    # Timestamp parse
    df = to_timestamp_safely(df, ["shipping_limit_date"])
    # Feature: order_value
    if set(["price", "freight_value"]).issubset(df.columns):
        df = df.withColumn("order_value", F.col("price") + F.col("freight_value"))

    preview("order_items (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: order_payments ----------
def clean_order_payments():
    name = "order_payments"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    df = df.filter(F.col("order_id").isNotNull())
    if "payment_value" in df.columns:
        df = df.filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
    if "payment_installments" in df.columns:
        df = df.filter(F.col("payment_installments").isNull() | (F.col("payment_installments") >= 0))
        df = df.withColumn("is_multiple_installments", F.when(F.col("payment_installments") > 1, 1).otherwise(0))
    # (Optional) keep common payment types
    if "payment_type" in df.columns:
        df = df.filter(
            F.col("payment_type").isin("credit_card","boleto","voucher","debit_card","not_defined") |
            F.col("payment_type").isNull()
        )

    preview("order_payments (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: order_reviews ----------
def clean_order_reviews():
    name = "order_reviews"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    # Essential IDs
    df = df.filter(F.col("order_id").isNotNull() & F.col("review_id").isNotNull())
    # Valid score range 1..5
    if "review_score" in df.columns:
        df = df.filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
    # Timestamps
    df = to_timestamp_safely(df, ["review_creation_date","review_answer_timestamp"])

    preview("order_reviews (cleaned)", df)
    write_out(df, f"{PRE}/{name}")

# ---------- table: orders ----------
def clean_orders():
    name = "orders"
    path = f"{RAW}/{name}"
    df = read_parquet(path)
    if df is None: return
    print(f"🧽 Cleaning: {name}")

    df = base_clean(df)
    df = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    # Accept only “active/fulfilled” statuses as in common EDAs
    if "order_status" in df.columns:
        df = df.filter(F.col("order_status").isin("delivered", "shipped", "invoiced", "approved"))
    # Parse times
    df = to_timestamp_safely(df, [
        "order_purchase_timestamp","order_approved_at",
        "order_delivered_carrier_date","order_delivered_customer_date",
        "order_estimated_delivery_date"
    ])
    # Filter unrealistic delivery durations (negative or > 300 days)
    if set(["order_purchase_timestamp","order_delivered_customer_date"]).issubset(df.columns):
        df = df.withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
        ).filter((F.col("delivery_days") >= 0) & (F.col("delivery_days") <= 300)).drop("delivery_days")

    # Features used later in EDA
    if set([
        "order_purchase_timestamp","order_delivered_customer_date","order_estimated_delivery_date"
    ]).issubset(df.columns):
        df = df.withColumn(
            "delivery_delay_days",
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")
        ).withColumn(
            "shipping_time_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
        ).withColumn(
            "is_late", F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)
        )

    # Partition for fast Athena queries
    if "order_purchase_timestamp" in df.columns:
        df = df.withColumn("order_year", F.year("order_purchase_timestamp")) \
               .withColumn("order_month", F.month("order_purchase_timestamp"))

    preview("orders (cleaned)", df)
    write_out(df, f"{PRE}/{name}", partitions=["order_year","order_month"] if "order_year" in df.columns else None)

# ---------- run all ----------
tables = [
    clean_customers,
    clean_sellers,
    clean_products,
    clean_geolocation,
    clean_order_items,
    clean_order_payments,
    clean_order_reviews,
    clean_orders
]

for fn in tables:
    print("\n==============================")
    print(f"▶️  {fn.__name__}")
    print("==============================")
    fn()

print("\n🎉 Cleaning finished. Data ready under preprocessed/ layer.")
job.commit()
