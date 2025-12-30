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
    
ROOT_DIR = Path(__file__).resolve().parents[2]  # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from env.config import (
    BLOB_CONNECTION_STRING,
    BRONZE_CONTAINER
)

from utils.data import (
    download_owl
)

from extractor import (
    extract
)

from utils.versioning import (
    extract_version,
    is_newer_version,
    update_manifest,
    update_latest_folder
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
    fileLocation = config.get("fileLocation")
    version_marker = config.get("version")

    if not fileLocation:
        raise ValueError("OLS payload does not expose a 'versionIri' field.")
    if not version_marker:
        raise ValueError("OLS payload does not expose a 'version' field for version tracking.")

    logger.info(f"Discovered {ontology_id.upper()} versionIri: %s | version: %s", fileLocation, version_marker)
    return fileLocation, version_marker

def should_run(ontology_id: str, endpoint: str, container: ContainerClient):
    endpoint = f"{endpoint}/{ontology_id}"
    response = requests.get(endpoint, timeout=15)
    response.raise_for_status()
    payload = response.json()
    remote_version = payload.get("version")
    stored_version = extract_version(ontology_id, container, logger)
    should_run = is_newer_version(remote_version, stored_version)
    return should_run

def run(ontology_id: str) -> None:
    logger.info(f"Starting {ontology_id.upper()} ontology synchronization workflow.")
    endpoint = f"https://www.ebi.ac.uk/ols4/api/ontologies"
    # Get metadata from OLS
    fileLocation, version_marker = fetch_version_metadata(endpoint, ontology_id)
    filename = fileLocation.split("/")[-1]
    # Run if newer version identified
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_client.get_container_client(BRONZE_CONTAINER)
    
    if should_run(ontology_id, endpoint, container):
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        container_url = download_owl(fileLocation=fileLocation, 
                                    ontology_id=ontology_id, 
                                    container_client=container, 
                                    version_marker=version_marker, 
                                    logger=logger)
        logger.info("Uploaded new ontology to: %s", container_url)
        # Update manifest with new version
        update_manifest(
            container=container, 
            source_id=ontology_id,
            hosts=[f"{endpoint}/{ontology_id}"], 
            version=version_marker, 
            update_ts=timestamp, 
            list_of_files=[f"raw/{ontology_id}/latest/{version_marker}/{filename}"],
            logger=logger)

        update_latest_folder(
            container=container,
            source_id=ontology_id,
            version=version_marker,
            logger=logger)
        
        extract(
            source_id=ontology_id,
            container=container,
            logger=logger
        )
        
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