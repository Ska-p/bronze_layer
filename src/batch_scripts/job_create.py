# create_recurring_job_schedule.py

import os
import sys

from datetime import timedelta
from pathlib import Path
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch import models as batch_models

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from env.config import (
    BATCH_ACCOUNT_NAME,
    BATCH_ACCOUNT_KEY,
    BATCH_ACCOUNT_URL,
)

POOL_ID = "bronze_pool"
SCHEDULE_ID = "data_sources_download"
CONTAINER_IMAGE = "dompedatafusiontest.azurecr.io/bronze_layer:latest"
JOB_MANAGER_COMMAND = "python src/job_manager/job_manager.py"
JOB_MANAGER_TASK_ID = "jobmanager"

def main():
    creds = SharedKeyCredentials(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY)
    client = BatchServiceClient(creds, batch_url=BATCH_ACCOUNT_URL)

    # Define the job specification for scheduled jobs
    job_spec = batch_models.JobSpecification(
        pool_info=batch_models.PoolInformation(pool_id=POOL_ID),
        job_manager_task=batch_models.JobManagerTask(
            id=JOB_MANAGER_TASK_ID,
            command_line=JOB_MANAGER_COMMAND,
            container_settings=batch_models.TaskContainerSettings(
                image_name=CONTAINER_IMAGE,
                container_run_options="--workdir /app",
                working_directory="containerImageDefault"
            ),
            user_identity=batch_models.UserIdentity(
                auto_user=batch_models.AutoUserSpecification(
                    scope="pool",
                    elevation_level=batch_models.ElevationLevel.non_admin
                )
            ),
            kill_job_on_completion=True
        )
    )

    # Create the schedule
    schedule = batch_models.Schedule(
        recurrence_interval=timedelta(minutes=5)
    )

    job_schedule = batch_models.JobScheduleAddParameter(
        id=SCHEDULE_ID,
        schedule=schedule,
        job_specification=job_spec
    )

    client.job_schedule.add(job_schedule)
    print(f"Created job schedule '{SCHEDULE_ID}' every 2 minutes on pool '{POOL_ID}'")

if __name__ == "__main__":
    main()
