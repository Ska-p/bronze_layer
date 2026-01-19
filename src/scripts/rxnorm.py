#!/usr/bin/env python3
"""Download latest RxNorm prescribable content if newer than stored version."""

import requests
import logging
import sys

from pathlib import Path
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    
ROOT_DIR = Path(__file__).resolve().parents[2]  # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

from utils.versioning import (
    update_latest_folder,
    update_manifest,
    is_newer_version,
    extract_version
)

from env.config import (
    BRONZE_CONTAINER,
    BLOB_CONNECTION_STRING
)

def download_latest_rxnorm():
    API_URL = "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-prescribable-content-monthly-release"
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("API")

    # Get latest release
    response = requests.get(API_URL)
    response.raise_for_status()
    latest = response.json()[0]

    # Check stored version
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_client.get_container_client(BRONZE_CONTAINER)
    stored_version = extract_version("rxnorm", container=container, logger=logger)

    # Download if newer
    if is_newer_version(local=stored_version, remote=latest["releaseDate"]):
        logger.info(f"Downloading {latest['fileName']}...")
        
        file_response = requests.get(latest['downloadUrl'], stream=True)
        file_response.raise_for_status()
        
        # Upload to blob storage
        blob_path = f"raw/rxnorm/latest/{latest['releaseDate']}/{latest['fileName']}"
        blob = container.get_blob_client(blob_path)
        blob.upload_blob(file_response.content, overwrite=True)
        
        logger.info(f"Uploaded to blob: {blob_path}")
        timestamp = datetime.now().strftime("%Y-%m-%d")

        # Update versioning
        update_manifest(
            source_id="rxnorm", 
            version=latest["releaseDate"],
            update_ts=timestamp,
            container=container, 
            logger=logger,
            hosts=[API_URL],
            list_of_files=[blob_path])
        
        update_latest_folder(
            source_id="rxnorm",
            version=latest["releaseDate"],
            container=container, 
            logger=logger)
        
    else:
        logger.info("Already up to date")
    
    
if __name__=="__main__":
    download_latest_rxnorm()