# ============================================================
# 🚀 AWS Glue Full ETL Pipeline Runner (Stable Rollback Version)
# ============================================================
# Runs:
#   1️⃣ Normalization  – Denormalized → Normalized tables
#   2️⃣ Cleaning       – Normalized → Preprocessed tables
#
# Features:
#   ✅ Simple, stable, Glue 5.0 compatible
#   ✅ Prints clear status of each job
#   ❌ No CloudWatch log streaming (runs faster & cleaner)
# ============================================================

import os
import sys
import time
import json
import boto3
import secrets

# --- Fix Python import path ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ------------------------------------------------------------
# Load configuration
# ------------------------------------------------------------
cfg = json.load(open("config/config.json"))
REGION = cfg["region"]
BUCKET = cfg["bucket_name"]
ROLE = cfg["glue_role"]

RAW_S3 = f"s3://{BUCKET}/raw/normalized"
PREPROCESSED_S3 = f"s3://{BUCKET}/preprocessed"
INPUT_PATH = cfg.get("input_s3_path", f"s3://{BUCKET}/input_file/denormalized_brazilian_dataset.csv")

# ------------------------------------------------------------
# Initialize AWS clients
# ------------------------------------------------------------
glue = boto3.client("glue", region_name=REGION)
s3   = boto3.client("s3", region_name=REGION)
sts  = boto3.client("sts", region_name=REGION)

account_id = sts.get_caller_identity()["Account"]
TMP_BUCKET  = f"aws-glue-scripts-{REGION}-{account_id}"

# ------------------------------------------------------------
# Ensure Glue script bucket exists
# ------------------------------------------------------------
def ensure_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Glue script bucket exists: {bucket_name}")
    except Exception:
        print(f"🪣 Creating Glue script bucket: {bucket_name} ...")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )

ensure_bucket_exists(TMP_BUCKET)

# ------------------------------------------------------------
# Helper: create, run, and delete temporary Glue job
# ------------------------------------------------------------
def run_glue_job(local_script_path, default_args):
    job_name = f"local_temp_{os.path.basename(local_script_path).split('.')[0]}_{secrets.token_hex(4)}"
    script_key = f"{job_name}.py"

    print(f"\n🚀 Uploading {local_script_path} to {TMP_BUCKET}/{script_key} ...")
    with open(local_script_path, "r") as f:
        s3.put_object(Bucket=TMP_BUCKET, Key=script_key, Body=f.read().encode("utf-8"))

    print(f"🧩 Creating Glue job {job_name} ...")
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

    print(f"🚀 Starting Glue job: {job_name}")
    resp = glue.start_job_run(JobName=job_name)
    run_id = resp["JobRunId"]
    print(f"✅ Glue job started. Run ID: {run_id}")

    # --- Wait until job finishes ---
    while True:
        time.sleep(15)
        status = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
        if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
            print(f"🏁 Glue job finished with status: {status}")
            break

    print(f"🧹 Deleting temporary Glue job {job_name} ...")
    glue.delete_job(JobName=job_name)
    print("✅ Cleanup complete.\n")

# ------------------------------------------------------------
# MAIN PIPELINE
# ------------------------------------------------------------
if __name__ == "__main__":
    print("\n======================================")
    print("🚀 Starting Full AWS Glue ETL Pipeline")
    print("======================================\n")

    # 1️⃣ NORMALIZATION
    print("[1/2] Running normalization job ...")
    norm_args = {
        "--JOB_NAME": "etl_normalization_job",
        "--source_path": INPUT_PATH,
        "--source_format": "csv",
        "--output_s3": f"s3://{BUCKET}/raw/normalized",
        "--aws_region": REGION,
        "--TempDir": f"s3://{BUCKET}/temp/"
    }

    run_glue_job("scripts/normalize_denormalized_to_s3.py", norm_args)
    print("✅ Normalization completed successfully!\n")

    # 2️⃣ CLEANING
    print("[2/2] Running cleaning job ...")
    clean_args = {
        "--JOB_NAME": "etl_cleaning_job",
        "--RAW_S3": RAW_S3,
        "--PREPROCESSED_S3": PREPROCESSED_S3,
        "--TempDir": f"s3://{BUCKET}/temp/"
    }

    run_glue_job("scripts/glue_cleaning_job.py", clean_args)
    print("✅ Cleaning completed successfully!\n")

    print("🎉 ETL Pipeline finished! Preprocessed data available under:")
    print(f"➡️  s3://{BUCKET}/preprocessed/")
