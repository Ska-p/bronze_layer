"""
Environment-driven configuration values shared across the project.

This module centralizes access to Azure credentials, container defaults,
and file-system paths so that other modules can import a single source
of truth regardless of whether they run locally or inside the Batch
container.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
SOURCES_CONFIG_PATH = CONFIG_DIR / "sources.yaml"

BLOB_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONNECTION_STRING")
BLOB_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_BLOB_STORAGE_ACCOUNT_NAME")
BRONZE_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER_BRONZE")
BATCH_ACCOUNT_NAME = os.getenv("AZ_BATCH_ACCOUNT_NAME")
BATCH_ACCOUNT_KEY = os.getenv("AZ_BATCH_ACCOUNT_KEY")
BATCH_ACCOUNT_URL = os.getenv("AZ_BATCH_ACCOUNT_URL")
JOB_ID = os.getenv("JOB_ID")
BRONZE_CONTAINER_IMAGE = os.getenv("BRONZE_CONTAINER_IMAGE")

DEFAULT_CONTAINER_WORKDIR = os.getenv("CONTAINER_WORKDIR", "/app")
DEFAULT_SCRIPT_TEMPLATE ="python src/scripts/{source}.py"
