import boto3, time, json, os
from botocore.exceptions import ClientError

# Load config
cfg = json.load(open("config/config.json"))
REGION = cfg["region"]
BUCKET = cfg["bucket_name"]
DATABASE = cfg["catalog_database"]
OUTPUT_S3 = f"s3://{BUCKET}/athena-results/"

athena = boto3.client("athena", region_name=REGION)

def run_athena_query(query, desc):
    print(f"\n📊 Running EDA Query: {desc}")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_S3}
    )
    qid = response["QueryExecutionId"]

    while True:
        time.sleep(5)
        result = athena.get_query_execution(QueryExecutionId=qid)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print(f"   → {state}")
            break

    if state == "SUCCEEDED":
        print(f"✅ Results stored in {OUTPUT_S3}{qid}.csv")
    else:
        print(f"⚠️ Query {desc} failed: {result}")

def perform_eda():
    print("\n==============================")
    print("📈 Starting Athena EDA Analysis")
    print("==============================\n")

    # Example EDA queries based on Kaggle Section 5
    queries = [
        (
            "Top 10 product categories by sales value",
            """
            SELECT 
                p.product_category_name,
                ROUND(SUM(CAST(oi.price AS DOUBLE)), 2) AS total_sales
            FROM order_items oi
            JOIN products p 
                ON oi.product_id = p.product_id
            GROUP BY p.product_category_name
            ORDER BY total_sales DESC
            LIMIT 10;

            """
        ),
        (
            "Average delivery delay (actual vs estimated)",
            """
            SELECT ROUND(AVG(date_diff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)),2) AS avg_delay_days
            FROM orders o
            WHERE o.order_delivered_customer_date IS NOT NULL;
            """
        ),
        (
            "Payment types distribution",
            """
            SELECT payment_type, COUNT(*) AS cnt
            FROM order_payments
            GROUP BY 1
            ORDER BY cnt DESC;
            """
        ),
        (
            "Average review score by category",
            """
            SELECT 
                p.product_category_name,
                ROUND(AVG(CAST(r.review_score AS DOUBLE)), 2) AS avg_score
            FROM order_reviews r
            JOIN order_items oi 
                ON r.order_id = oi.order_id
            JOIN products p 
                ON oi.product_id = p.product_id
            GROUP BY p.product_category_name
            ORDER BY avg_score DESC
            LIMIT 10;
            """
        )
    ]

    for desc, q in queries:
        run_athena_query(q, desc)

    print("\n🎉 Athena EDA completed successfully!")

if __name__ == "__main__":
    perform_eda()
