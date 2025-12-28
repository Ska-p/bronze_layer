import logging
import requests
import argparse
import sys

from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient, ContainerClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.config import (
    BLOB_CONNECTION_STRING,
    BRONZE_CONTAINER
)

from utils.data import (
    download_owl
)

from utils.versioning import (
    update_manifest_latest_version_ols,
    update_ols_folder_versioning,
    extract_ols_version,
    is_newer_version
)

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("OLS")

def fetch_version_metadata(endpoint: str, ontology_id: str) -> Tuple[str, str]:
    endpoint = f"{endpoint}/{ontology_id}"
    response = requests.get(endpoint, timeout=30)
    response.raise_for_status()
    payload = response.json()
    config = payload.get("config")
    version_iri = config.get("versionIri")
    version_marker = config.get("version")

    if not version_iri:
        raise ValueError("OLS payload does not expose a 'versionIri' field.")
    if not version_marker:
        raise ValueError("OLS payload does not expose a 'version' field for version tracking.")

    logger.info(f"Discovered {ontology_id.upper()} versionIri: %s | version: %s", version_iri, version_marker)
    return version_iri, version_marker

def should_run(ontology_id: str, endpoint: str, blob_client: BlobServiceClient):
    endpoint = f"{endpoint}/{ontology_id}"
    response = requests.get(endpoint, timeout=15)
    response.raise_for_status()
    payload = response.json()
    remote_version = payload.get("version")
    stored_version = extract_ols_version(ontology_id, blob_client, logger)
    should_run = is_newer_version(remote_version, stored_version)
    print(remote_version, stored_version)
    print(should_run)
    return should_run

def run(ontology_id: str) -> None:
    logger.info(f"Starting {ontology_id.upper()} ontology synchronization workflow.")
    endpoint = f"https://www.ebi.ac.uk/ols4/api/ontologies"
    # Get metadata from OLS
    version_iri, version_marker = fetch_version_metadata(endpoint, ontology_id)
    # Run if newer version identified
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_client.get_container_client(BRONZE_CONTAINER)
    
    if should_run(ontology_id, endpoint, container):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
        container_url, suffix = download_owl(version_iri, ontology_id, container, timestamp, logger)
        logger.info("Uploaded new ontology to: %s", container_url)
        # Update manifest with new version
        latest_prefix = f"raw/{ontology_id}/latest"
        update_manifest_latest_version_ols(container, ontology_id, version_iri, version_marker, timestamp, latest_prefix, logger)
        update_ols_folder_versioning(container, ontology_id, timestamp, logger)
        logger.info(f"{ontology_id.upper()} ontology sync completed successfully!")
    else:
        logger.info(f"{ontology_id.upper()} ontology up to date.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline with a given identifier")
    parser.add_argument(
        "-id",
        "--id",
        required=True,
        help="Identifier to select configuration/template"
    )
    args = parser.parse_args()
    ontology_id = args.id
    run(ontology_id)