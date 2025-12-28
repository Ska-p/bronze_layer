from __future__ import annotations

import os
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

import yaml
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch import models as batch_models
from azure.batch.custom.custom_errors import CreateTasksErrorException

from config.config import (
    BATCH_ACCOUNT_KEY,
    BATCH_ACCOUNT_NAME,
    BATCH_ACCOUNT_URL,
    BRONZE_CONTAINER_IMAGE,
    SOURCES_CONFIG_PATH,
)

JOB_ID = os.environ["AZ_BATCH_JOB_ID"]

def load_sources_config(
    config_path: Path = SOURCES_CONFIG_PATH,
) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Sources configuration not found at {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle) or {}

    sources = document.get("sources")
    if not isinstance(sources, dict):
        raise ValueError("'sources' must be a mapping")
    return sources

def build_command_line(group: str, source_id: str) -> str:
    if group == "custom":
        return f"python src/scripts/{source_id}.py"
    else:
        return f"python src/scripts/{group}.py -id {source_id}"

def build_task(group: str, source_id: str) -> batch_models.TaskAddParameter:
    command_line = build_command_line(group, source_id)

    container_settings = batch_models.TaskContainerSettings(
        image_name=BRONZE_CONTAINER_IMAGE,
        container_run_options="--workdir /app",
        working_directory=batch_models.ContainerWorkingDirectory.container_image_default,
    )

    return batch_models.TaskAddParameter(
        id=f"{group}_{source_id}",
        command_line=command_line,
        container_settings=container_settings,
        user_identity=batch_models.UserIdentity(
            auto_user=batch_models.AutoUserSpecification(
                scope="pool",
                elevation_level=batch_models.ElevationLevel.non_admin,
            )
        ),
    )

def enumerate_sources(sources: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
    for group, payload in sources.items():

        if group == "ols":
            if not isinstance(payload, list):
                raise ValueError("ols must be a list")
            for source_id in payload:
                yield group, source_id
        elif group == "custom":
            if not isinstance(payload, list):
                raise ValueError("custom must be a list")
            for source_id in payload:
                yield group, source_id
        else:
            if not isinstance(payload, dict):
                raise ValueError(f"{group} must be a mapping")
            for source_id in payload.keys():
                yield group, source_id

def submit_tasks(
    client: BatchServiceClient,
    tasks: Iterable[batch_models.TaskAddParameter],
) -> None:
    task_list = list(tasks)
    if not task_list:
        logging.info("No tasks to submit.")
        return

    try:
        client.task.add_collection(JOB_ID, task_list)
        logging.info("Submitted %d tasks to job %s.", len(task_list), JOB_ID)

    except CreateTasksErrorException as exc:
        logging.error("Failed to add tasks.")
        if exc.failure_tasks:
            for ft in exc.failure_tasks:
                logging.error("Task %s failed: %s", ft.task_id, ft.error)
        raise

def create_batch_client() -> BatchServiceClient:
    if not all(
        [
            BATCH_ACCOUNT_NAME,
            BATCH_ACCOUNT_KEY,
            BATCH_ACCOUNT_URL,
            JOB_ID,
            BRONZE_CONTAINER_IMAGE,
        ]
    ):
        raise RuntimeError("Batch configuration incomplete")

    credentials = SharedKeyCredentials(
        BATCH_ACCOUNT_NAME,
        BATCH_ACCOUNT_KEY,
    )
    return BatchServiceClient(credentials, batch_url=BATCH_ACCOUNT_URL)

def main() -> None:
    sources = load_sources_config()
    client = create_batch_client()

    tasks = (
        build_task(group, source_id)
        for group, source_id in enumerate_sources(sources)
    )

    submit_tasks(client, tasks)

if __name__ == "__main__":
    main()