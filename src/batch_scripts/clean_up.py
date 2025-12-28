# cleanup_jobs_in_pool.py

import os
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials

# Config — adjust or load from your config module/env
from config.config import (
    BATCH_ACCOUNT_KEY,
    BATCH_ACCOUNT_NAME,
    BATCH_ACCOUNT_URL
    )

TARGET_POOL_ID = "bronze_pool"  # change to your pool id

def main():
    creds = SharedKeyCredentials(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY)
    client = BatchServiceClient(creds, batch_url=BATCH_ACCOUNT_URL)

    jobs = client.job.list()  # list all jobs
    jobs_to_delete = [j for j in jobs if j.pool_info and j.pool_info.pool_id == TARGET_POOL_ID]

    if not jobs_to_delete:
        print(f"No jobs found for pool '{TARGET_POOL_ID}'")
        return

    print(f"Found {len(jobs_to_delete)} jobs in pool '{TARGET_POOL_ID}': {[j.id for j in jobs_to_delete]}")

    for j in jobs_to_delete:
        try:
            client.job.delete(j.id)
            print(f"Deleted job {j.id}")
        except Exception as e:
            print(f"Failed to delete job {j.id}: {e}")

if __name__ == "__main__":
    main()