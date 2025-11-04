import boto3, time, json, secrets
from urllib.parse import urlparse


# --- Load config ---
cfg = json.load(open("config/config.json"))
REGION = cfg["region"]
BUCKET = cfg["bucket_name"]
ROLE = cfg["glue_role"]
SOURCE_PATH = cfg.get("source_path")
if not SOURCE_PATH or not SOURCE_PATH.startswith("s3://"):
    raise ValueError("Invalid or missing 'source_path' in config/config.json")

OUTPUT_S3 = f"s3://{BUCKET}/raw/normalized"
job_name = f"local_temp_normalize_{secrets.token_hex(4)}"

# --- Initialize AWS clients ---
glue = boto3.client("glue", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
sts = boto3.client("sts")

account_id = sts.get_caller_identity()["Account"]
tmp_bucket = f"aws-glue-scripts-{REGION}-{account_id}"

# --- Ensure Glue script bucket exists ---
try:
    s3.head_bucket(Bucket=tmp_bucket)
except Exception:
    print(f"🪣 Glue script bucket {tmp_bucket} not found, creating it...")
    s3.create_bucket(
        Bucket=tmp_bucket,
        CreateBucketConfiguration={"LocationConstraint": REGION}
    )

# --- Verify input file exists ---
parsed_input = urlparse(SOURCE_PATH)
in_bucket = parsed_input.netloc
in_key = parsed_input.path.lstrip("/")
print(f"🪣 Checking input file: {SOURCE_PATH}")
try:
    s3.head_object(Bucket=in_bucket, Key=in_key)
    print("✅ Input file exists.")
except Exception:
    raise FileNotFoundError(f"❌ Input file not found in S3: {SOURCE_PATH}")

# --- Ensure output folder exists ---
parsed = urlparse(OUTPUT_S3 if OUTPUT_S3.endswith('/') else OUTPUT_S3 + '/')
out_bucket = parsed.netloc
out_prefix = parsed.path.lstrip("/")
print(f"🪣 Checking output path: s3://{out_bucket}/{out_prefix}")
try:
    resp = s3.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix, MaxKeys=1)
    if 'Contents' not in resp:
        print(f"📁 Creating output prefix {out_prefix} in bucket {out_bucket}...")
        s3.put_object(Bucket=out_bucket, Key=f"{out_prefix}")
except Exception as e:
    print(f"⚠️ Could not verify/create output path: {e}")

# --- Upload Glue script ---
print(f"🚀 Uploading normalization script to {tmp_bucket}...")
with open("scripts/normalize_denormalized_to_s3.py") as f:
    s3.put_object(Bucket=tmp_bucket, Key=f"{job_name}.py", Body=f.read().encode("utf-8"))

# --- Create Glue job ---
print(f"🚀 Creating Glue job {job_name}...")
try:
    glue.create_job(
        Name=job_name,
        Role=ROLE,
        Command={"Name":"glueetl","PythonVersion":"3","ScriptLocation":f"s3://{tmp_bucket}/{job_name}.py"},
        GlueVersion="5.0",
        DefaultArguments={
            "--JOB_NAME": job_name,
            "--source_path": SOURCE_PATH,
            "--source_format": "csv",
            "--output_s3": OUTPUT_S3,
            "--aws_region": REGION,
            "--TempDir": f"s3://{BUCKET}/temp/"
        },
        MaxCapacity=2.0
    )
except glue.exceptions.AlreadyExistsException:
    print("ℹ️ Job already exists, reusing it.")

# --- Start Glue job ---
resp = glue.start_job_run(JobName=job_name)
run_id = resp["JobRunId"]
print(f"✅ Started Glue normalization job. Run ID: {run_id}")

# --- Monitor job ---
while True:
    status = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
    if status in ["SUCCEEDED", "FAILED", "STOPPED"]:
        print(f"🏁 Job finished with status: {status}")
        break
    print(f"⏳ Status: {status}")
    time.sleep(15)

# --- Cleanup ---
# print(f"🧹 Deleting temporary Glue job {job_name}...")
# glue.delete_job(JobName=job_name)
print("✅ Cleanup complete.")
