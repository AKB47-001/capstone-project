# ============================================================
# 🪶 Glue Log Stream Utility (final version)
# Streams Glue job logs in real-time to local terminal
# Uses the actual LogStreamName for accurate CloudWatch output
# ============================================================

import time
import botocore.exceptions

def stream_glue_logs(glue_client, logs_client, job_name, run_id, poll_interval=10):
    """
    Stream AWS Glue job logs from CloudWatch to the local terminal in real-time.
    Works reliably by using the actual Glue LogStreamName.
    """

    print("\n📜 Waiting for CloudWatch logs to appear...")
    log_group = "/aws-glue/jobs/output"
    log_stream = None
    start_time = int(time.time() * 1000)
    seen_events = set()

    # Step 1️⃣ — Wait until Glue returns log stream info
    for _ in range(30):
        try:
            job_run = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            log_stream = job_run["JobRun"].get("LogStreamName")
            if log_stream:
                print(f"✅ Found log stream: {log_stream}")
                break
        except Exception:
            pass
        time.sleep(5)
    if not log_stream:
        print("⚠️ Could not find log stream, falling back to filter_log_events method.")
        return

    print(f"📡 Streaming from CloudWatch Log Group: {log_group}")
    print("----------------------------------------------------\n")

    next_token = None

    # Step 2️⃣ — Keep polling until job ends
    while True:
        try:
            kwargs = {"logGroupName": log_group, "logStreamName": log_stream, "startTime": start_time}
            if next_token:
                kwargs["nextToken"] = next_token

            response = logs_client.get_log_events(**kwargs)
            for event in response.get("events", []):
                msg = event["message"].rstrip()
                if msg not in seen_events:
                    print(msg)
                    seen_events.add(msg)

            next_token = response.get("nextForwardToken")

            # check Glue job state
            job_state = glue_client.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
            if job_state in ["SUCCEEDED", "FAILED", "STOPPED"]:
                print(f"\n🏁 Glue job finished with status: {job_state}")
                break

            time.sleep(poll_interval)

        except botocore.exceptions.ClientError as e:
            if "ThrottlingException" in str(e):
                time.sleep(5)
                continue
            print(f"⚠️ Log stream error: {e}")
            break
        except Exception as e:
            print(f"⚠️ Unexpected streaming error: {e}")
            break
