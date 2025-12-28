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

def upload_file_to_blob(blob_client: BlobClient, file_path: str, logger: logging.Logger, overwrite: bool = True) -> None:
    with open(file_path, "rb") as local_stream:
        blob_client.upload_blob(local_stream, overwrite=overwrite)
    logger.info("Uploaded file %s to blob %s", file_path, blob_client.blob_name)

def delete_blob_if_exists(blob_client: BlobClient) -> None:
    try:
        blob_client.delete_blob()
    except ResourceNotFoundError:
        return

def get_newest_blob(container: ContainerClient, prefix: str, logger: logging.Logger) -> BlobProperties | None:
    blobs = [
        blob for blob in container.list_blobs(name_starts_with=prefix)
        if not blob.name.endswith("/")
    ]
    if not blobs:
        logger.info("No blobs found with prefix %s", prefix)
        return None
    blobs.sort(key=lambda item: item.last_modified or datetime.min, reverse=True)
    return blobs[0]

def copy_blob_within_container(container: ContainerClient, source_name: str, destination_name: str, logger: logging.Logger) -> None:
    source_client = create_blob_client(container, source_name)
    destination_client = create_blob_client(container, destination_name)
    copy_id = destination_client.start_copy_from_url(source_client.url)
    props = destination_client.get_blob_properties()
    while props.copy.status == "pending":
        time.sleep(1)
        props = destination_client.get_blob_properties()
    if props.copy.status != "success":
        try:
            destination_client.abort_copy(copy_id)
        except Exception:
            pass
        raise RuntimeError(f"Copy from {source_name} to {destination_name} failed with status {props.copy.status}")
    source_client.delete_blob()
    logger.info("Copied blob %s to %s", source_name, destination_name)

def _suffix_from_url(url: str) -> str:
    """
    Infer file suffix from the download URL.
    Example: "hp.owl.gz" -> "owl.gz"
    """
    path = urlparse(url).path
    return path.split("/")[-1].split(".", 1)[-1]

def download_and_upload_web_based(
    url: str,
    source_id: str,
    container: ContainerClient,
    logger: logging.Logger
) -> None:
    filename = Path(urlparse(url).path).name
    blob_path = f"raw/{source_id}/latest/{filename}"
    logger.info(f"Streaming download: {url}")

    with requests.get(url, stream=True, timeout=(10, 600)) as resp:
        resp.raise_for_status()
        size = resp.headers.get("Content-Length")
    
        if size:
            logger.info(f"Expected size: {int(size) / (1024**2):.1f} MB")
            
        blob_client = container.get_blob_client(blob_path)
        blob_client.upload_blob(
            data=resp.raw,
            overwrite=True
        )

    logger.info(f"Uploaded → {blob_path}")
    return blob_path


def download_owl(version_iri: str, ontology_id: str, container_client: ContainerClient, timestamp: str, logger: logging.Logger, extension: str = ".owl") -> Tuple[str, str]:
    """
    Download OWL (or compressed OWL) file directly into Azure Blob Storage.

    Storage path:
      bronzelayer/raw/{ontology_id}/latest/YYYYMMDD_HHMMSS_{ontology_id}.owl

    Returns:
        blob_name, suffix
    """
    blob_name = f"raw/{ontology_id}/latest/{timestamp}_{ontology_id}.{extension}"

    suffix = _suffix_from_url(version_iri)

    logger.info("Requesting download: %s", version_iri)
    response = requests.get(version_iri, stream=True, timeout=120)
    response.raise_for_status()

    # Blob client from connection string
    blob_client = container_client.get_blob_client(blob=blob_name)

    logger.info("Uploading stream to blob: bronzelayer/%s", blob_name)
    blob_client.upload_blob(
        data=response.iter_content(chunk_size=8192),
        overwrite=True
    )
    
    logger.info("Upload completed")
    return blob_name, suffix