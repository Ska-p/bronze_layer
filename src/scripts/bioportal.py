import logging
import requests
import argparse
import sys

from pathlib import Path
from typing import Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient, ContainerClient

# ==============================================================================
# Path bootstrap (consistent with your other scripts)
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parents[1]   # /app/src
ROOT_DIR = Path(__file__).resolve().parents[2]   # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# ==============================================================================
# Config
# ==============================================================================

from env.config import (
    BLOB_CONNECTION_STRING,
    BRONZE_CONTAINER,
    BIOPORTAL_API_KEY
)

# ==============================================================================
# Internal utilities
# ==============================================================================

from extractor import extract

from utils.versioning import (
    extract_version,
    is_newer_version,
    update_manifest,
    update_latest_folder
)

# ==============================================================================
# Logging
# ==============================================================================

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("BIOPORTAL")

# ==============================================================================
# Constants
# ==============================================================================

BIOPORTAL_BASE = "https://data.bioontology.org"
ONTOLOGY_ENDPOINT = f"{BIOPORTAL_BASE}/ontologies"

HEADERS = {
    "Authorization": f"apikey token={BIOPORTAL_API_KEY}",
    "Accept": "application/json"
}

# ==============================================================================
# Metadata resolution
# ==============================================================================

def fetch_bioportal_metadata(ontology_id: str) -> Tuple[str, str, str]:
    """
    Resolve BioPortal metadata.

    Returns:
        download_url (str)
        version_marker (YYYY-MM-DD)
        latest_submission_url (str)
    """

    ontology_url = f"{ONTOLOGY_ENDPOINT}/{ontology_id}"
    logger.info("Querying BioPortal ontology endpoint: %s", ontology_url)

    r = requests.get(ontology_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    payload = r.json()

    latest_submission_url = payload["links"]["latest_submission"]

    logger.info("Querying latest submission: %s", latest_submission_url)

    r = requests.get(latest_submission_url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    submission = r.json()

    creation_date_raw = submission.get("creationDate")
    if not creation_date_raw:
        raise ValueError("BioPortal submission does not expose creationDate")

    version_marker = (
        datetime.fromisoformat(creation_date_raw.replace("Z", "+00:00"))
        .date()
        .isoformat()
    )

    download_url = submission["ontology"]["links"]["download"]

    logger.info(
        "Resolved ontology=%s | version=%s | download=%s",
        ontology_id,
        version_marker,
        download_url
    )

    return download_url, version_marker, latest_submission_url

# ==============================================================================
# Version check
# ==============================================================================

def should_run(
    ontology_id: str,
    remote_version: str,
    container: ContainerClient
) -> bool:
    stored_version = extract_version(
        source_id=ontology_id,
        container=container,
        logger=logger
    )
    return is_newer_version(remote_version, stored_version)

# ==============================================================================
# Generic downloader (NO format assumptions)
# ==============================================================================

def download_and_upload(
    *,
    url: str,
    ontology_id: str,
    container: ContainerClient,
    version: str,
    logger: logging.Logger
) -> str:
    """
    Stream download from BioPortal and upload to Blob Storage.
    Does not assume OWL / TTL / RDF format.
    """

    logger.info("Downloading artifact from: %s", url)

    with requests.get(
        url,
        headers=HEADERS,
        stream=True,
        timeout=120
    ) as r:
        r.raise_for_status()

        # Filename resolution priority:
        # 1. Content-Disposition
        # 2. URL path
        # 3. Fallback
        filename = None
        cd = r.headers.get("Content-Disposition")
        if cd and "filename=" in cd:
            filename = cd.split("filename=")[-1].strip('"')

        if not filename:
            parsed = urlparse(url)
            filename = Path(parsed.path).name or f"{ontology_id}.ttl"

        blob_path = f"raw/{ontology_id}/latest/{version}/{filename}"
        blob_client = container.get_blob_client(blob_path)

        blob_client.upload_blob(
            r.raw,
            overwrite=True
        )

        logger.info("Uploaded blob to: %s", blob_path)
        return blob_path

# ==============================================================================
# Main pipeline
# ==============================================================================

def run(ontology_id: str) -> None:
    logger.info("Starting BIOPORTAL ontology sync: %s", ontology_id.upper())

    blob_service = BlobServiceClient.from_connection_string(
        BLOB_CONNECTION_STRING
    )
    container = blob_service.get_container_client(BRONZE_CONTAINER)

    download_url, version_marker, submission_url = fetch_bioportal_metadata(
        ontology_id
    )

    if not should_run(ontology_id, version_marker, container):
        logger.info("%s ontology already up to date.", ontology_id.upper())
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    blob_relative_path = download_and_upload(
        url=download_url,
        ontology_id=ontology_id,
        container=container,
        version=version_marker,
        logger=logger
    )

    update_manifest(
        container=container,
        source_id=ontology_id,
        hosts=[submission_url],
        version=version_marker,
        update_ts=timestamp,
        list_of_files=[blob_relative_path],
        logger=logger
    )

    update_latest_folder(
        container=container,
        source_id=ontology_id,
        version=version_marker,
        logger=logger
    )

    extract(
        source_id=ontology_id,
        container=container,
        logger=logger
    )

    logger.info("%s ontology sync completed successfully.", ontology_id.upper())

# ==============================================================================
# CLI
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run BioPortal ontology ingestion"
    )
    parser.add_argument(
        "-id",
        "--id",
        required=True,
        help="BioPortal ontology identifier (e.g. hp, doid, chebi)"
    )

    args = parser.parse_args()
    run(args.id)
