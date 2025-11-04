import boto3, time, json, secrets
from urllib.parse import urlparse


# --------------------------------------------------------
# Load configuration
# --------------------------------------------------------
cfg = json.load(open("config/config.json"))
REGION = cfg["region"]
BUCKET = cfg["bucket_name"]
ROLE = cfg["glue_role"]

RAW_S3 = f"s3://{BUCKET}/raw/normalized"
PREPROCESSED_S3 = f"s3://{BUCKET}/preprocessed"
job_name = f"local_temp_cleaning_{secrets.token_hex(4)}"

# --------------------------------------------------------
# Initialize AWS clients
# --------------------------------------------------------
glue = boto3.client("glue", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sts = boto3.client("sts", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

account_id = sts.get_caller_identity()["Account"]
tmp_bucket = f"aws-glue-scripts-{REGION}-{account_id}"

# --------------------------------------------------------
# Ensure Glue script bucket exists
# --------------------------------------------------------
try:
    s3.head_bucket(Bucket=tmp_bucket)
except Exception:
    print(f"🪣 Glue script bucket {tmp_bucket} not found, creating it...")
    s3.create_bucket(
        Bucket=tmp_bucket,
        CreateBucketConfiguration={"LocationConstraint": REGION}
    )

# --------------------------------------------------------
# Upload cleaning script
# --------------------------------------------------------
print(f"🚀 Uploading cleaning script to {tmp_bucket}...")
with open("scripts/glue_cleaning_job.py") as f:
    s3.put_object(Bucket=tmp_bucket, Key=f"{job_name}.py", Body=f.read().encode("utf-8"))

# --------------------------------------------------------
# Create Glue job
# --------------------------------------------------------
print(f"🧩 Creating Glue job {job_name}...")
try:
    glue.create_job(
        Name=job_name,
        Role=ROLE,
        Command={
            "Name": "glueetl",
            "PythonVersion": "3",
            "ScriptLocation": f"s3://{tmp_bucket}/{job_name}.py"
        },
        GlueVersion="5.0",
        DefaultArguments={
            "--JOB_NAME": job_name,
            "--RAW_S3": RAW_S3,
            "--PREPROCESSED_S3": PREPROCESSED_S3,
            "--TempDir": f"s3://{BUCKET}/temp/"
        },
        MaxCapacity=2.0
    )
except glue.exceptions.AlreadyExistsException:
    print("ℹ️ Job already exists, reusing it.")

# --------------------------------------------------------
# Start Glue job
# --------------------------------------------------------
print(f"🚀 Starting Glue job: {job_name}")
resp = glue.start_job_run(JobName=job_name)
run_id = resp["JobRunId"]
print(f"✅ Glue job started. Run ID: {run_id}")

# --------------------------------------------------------
# Helper: Stream Glue logs to terminal
# --------------------------------------------------------
def stream_glue_logs(run_id, log_group="/aws-glue/jobs/output"):
    """
    Stream Glue job logs in real-time to the local console.
    Works with Boto3 v1.34+ and handles pagination correctly.
    """
    import time
    import botocore.exceptions

    print("\n📜 Streaming Glue logs to local terminal (live)...\n")
    start_time = int(time.time() * 1000)
    next_token = None
    seen_messages = set()

    while True:
        try:
            params = {
                "logGroupName": log_group,
                "filterPattern": run_id,
                "startTime": start_time
            }
            if next_token:
                params["nextToken"] = next_token

            resp = logs.filter_log_events(**params)

            for event in resp.get("events", []):
                msg = event["message"].rstrip()
                # Deduplicate messages (CloudWatch can re-send)
                if msg not in seen_messages:
                    print(msg)
                    seen_messages.add(msg)

            next_token = resp.get("nextToken")

            # check Glue job state
            status = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
            if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
                print(f"\n🏁 Glue job finished with status: {status}")
                break

            time.sleep(10)

        except botocore.exceptions.ClientError as e:
            if "ThrottlingException" in str(e):
                time.sleep(5)
                continue
            print(f"⚠️ Log streaming error: {e}")
            break
        except Exception as e:
            print(f"⚠️ Unexpected error while streaming logs: {e}")
            break
# --------------------------------------------------------
# Stream logs until job completes
# --------------------------------------------------------
stream_glue_logs(run_id)

# --------------------------------------------------------
# Cleanup temporary Glue job
# --------------------------------------------------------
print(f"🧹 Deleting temporary Glue job {job_name}...")
# glue.delete_job(JobName=job_name)
print("✅ Cleanup complete.")
