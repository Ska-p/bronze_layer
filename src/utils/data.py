import json
import logging
import time
import requests

from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple, List
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobProperties
from azure.core.exceptions import ResourceNotFoundError


def create_blob_client(container: ContainerClient, path: str) -> BlobClient:
    return container.get_blob_client(path)

def load_manifest(blob_client: BlobClient, logger: logging.Logger) -> dict:
    try:
        data = blob_client.download_blob().readall()
        return json.loads(data)
    except ResourceNotFoundError:
        logger.info("Manifest blob %s not found; initializing empty manifest.", blob_client.blob_name)
        return {}
    except Exception as exc:
        logger.warning("Unable to load manifest from blob %s: %s", blob_client.blob_name, exc)
        return {}

def _suffix_from_url(url: str) -> str:
    """
    Infer file suffix from the download URL.
    Example: "hp.owl.gz" -> "owl.gz"
    """
    path = urlparse(url).path
    return path.split("/")[-1].split(".", 1)[-1]

def download_owl(fileLocation: str, ontology_id: str, container_client: ContainerClient, version_marker: str, logger: logging.Logger) -> str:
    """
    Download OWL (or compressed OWL) file directly into Azure Blob Storage.

    Storage path:
      bronzelayer/raw/{ontology_id}/latest/YYYYMMDD_HHMMSS_{ontology_id}.owl

    Returns:
        blob_name, suffix
    """
    filename = fileLocation.split("/")[-1]
    blob_name = f"raw/{ontology_id}/latest/{version_marker}/{filename}"

    suffix = _suffix_from_url(fileLocation)

    logger.info("Requesting download: %s", fileLocation)
    response = requests.get(fileLocation, stream=True, timeout=120)
    response.raise_for_status()

    # Blob client from connection string
    blob_client = container_client.get_blob_client(blob=blob_name)

    logger.info("Uploading stream to blob: bronzelayer/%s", blob_name)
    blob_client.upload_blob(
        data=response.iter_content(chunk_size=8192),
        overwrite=True
    )
    
    logger.info("Upload completed")
    return blob_name