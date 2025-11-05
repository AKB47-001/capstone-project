# ============================================================
# 🚀 AWS Glue Full ETL Pipeline Runner (with Glue Crawler step)
# ============================================================

import os, sys, time, json, boto3, secrets

# allow "from project root" or direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.athena_eda_runner import perform_eda

# ---------------- Config ----------------
cfg = json.load(open("config/config.json"))

REGION       = cfg["region"]
BUCKET       = cfg["bucket_name"]
ROLE         = cfg["glue_role"]
INPUT_PATH   = cfg.get("input_s3_path", f"s3://{BUCKET}/input_file/denormalized_brazilian_dataset.csv")
CATALOG_DB   = cfg.get("catalog_database", "brazilian_ecommerce_db")
CRAWLER_NAME = cfg.get("crawler_name", "g16-preprocessed-crawler")

RAW_S3          = f"s3://{BUCKET}/raw/normalized"
PREPROCESSED_S3 = f"s3://{BUCKET}/preprocessed"

# ---------------- AWS Clients ----------------
glue = boto3.client("glue", region_name=REGION)
s3   = boto3.client("s3", region_name=REGION)
sts  = boto3.client("sts", region_name=REGION)
account_id = sts.get_caller_identity()["Account"]
TMP_BUCKET  = f"aws-glue-scripts-{REGION}-{account_id}"

def ensure_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket exists: {bucket_name}")
    except Exception:
        print(f"🪣 Creating bucket: {bucket_name}")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )

def run_glue_job(local_script_path, default_args):
    script_name = os.path.splitext(os.path.basename(local_script_path))[0]
    job_name = f"kaushal's_{script_name}_{secrets.token_hex(4)}"
    script_key = f"{job_name}.py"

    print(f"\n📤 Uploading script → s3://{TMP_BUCKET}/{script_key}")
    with open(local_script_path, "r") as f:
        s3.put_object(Bucket=TMP_BUCKET, Key=script_key, Body=f.read().encode("utf-8"))

    print(f"🧩 Creating Glue job: {job_name}")
    glue.create_job(
        Name=job_name,
        Role=ROLE,
        Command={
            "Name": "glueetl",
            "PythonVersion": "3",
            "ScriptLocation": f"s3://{TMP_BUCKET}/{script_key}",
        },
        GlueVersion="5.0",
        DefaultArguments=default_args,
        MaxCapacity=2.0,
    )

    print(f"🚀 Starting job: {job_name}")
    run_id = glue.start_job_run(JobName=job_name)["JobRunId"]
    print(f"   RunId: {run_id}")

    while True:
        time.sleep(15)
        state = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
        if state in ["SUCCEEDED","FAILED","STOPPED"]:
            print(f"🏁 {job_name} → {state}")
            break

    print(f"🧹 Deleting temporary job: {job_name}")
    # glue.delete_job(JobName=job_name)
    return state

def ensure_crawler_and_run():
    # ensure database exists
    try:
        glue.get_database(Name=CATALOG_DB)
        print(f"✅ Glue database exists: {CATALOG_DB}")
    except glue.exceptions.EntityNotFoundException:
        print(f"📚 Creating Glue database: {CATALOG_DB}")
        glue.create_database(DatabaseInput={"Name": CATALOG_DB})

    # ensure crawler exists
    try:
        glue.get_crawler(Name=CRAWLER_NAME)
        print(f"✅ Crawler exists: {CRAWLER_NAME}")
    except glue.exceptions.EntityNotFoundException:
        print(f"🪄 Creating crawler: {CRAWLER_NAME}")
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=ROLE,
            DatabaseName=CATALOG_DB,
            Targets={"S3Targets": [{"Path": PREPROCESSED_S3}]},
            SchemaChangePolicy={"UpdateBehavior":"UPDATE_IN_DATABASE","DeleteBehavior":"LOG"}
        )

    print(f"🏃 Starting crawler: {CRAWLER_NAME}")
    glue.start_crawler(Name=CRAWLER_NAME)

    # wait for crawler to finish
    while True:
        time.sleep(15)
        state = glue.get_crawler(Name=CRAWLER_NAME)["Crawler"]["State"]
        if state == "READY":
            last_status = glue.get_crawler_metrics(CrawlerNameList=[CRAWLER_NAME])["CrawlerMetricsList"][0]["LastRuntimeSeconds"]
            print(f"✅ Crawler completed. (LastRuntimeSeconds={last_status})")
            break

if __name__ == "__main__":
    print("\n==========================")
    print("🚀 Starting ETL Pipeline")
    print("==========================\n")

    ensure_bucket_exists(TMP_BUCKET)

    # 1) Normalization (no partitioning)
    norm_args = {
        "--JOB_NAME": "etl_normalization_job",
        "--source_path": INPUT_PATH,
        "--source_format": "csv",
        "--output_s3": RAW_S3,
        "--aws_region": REGION,
        "--TempDir": f"s3://{BUCKET}/temp/"
    }
    run_glue_job("scripts/normalize_denormalized_to_s3.py", norm_args)

    # 2) Cleaning
    clean_args = {
        "--JOB_NAME": "etl_cleaning_job",
        "--RAW_S3": RAW_S3,
        "--PREPROCESSED_S3": PREPROCESSED_S3,
        "--TempDir": f"s3://{BUCKET}/temp/"
    }
    run_glue_job("scripts/glue_cleaning_job.py", clean_args)

    # 3) Crawler for Athena/QuickSight (cloud-only EDA)
    ensure_crawler_and_run()
    # 4) Run Athena EDA automatically    
    perform_eda()

    print("\n🎉 ETL + Cataloging finished. Query in Athena / build dashboards in QuickSight.")
