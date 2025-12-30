import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import json

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient

from utils.data import (
    load_manifest,
    create_blob_client
)

MANIFEST_BLOB_NAME = "manifest.json"

def extract_version(source_id:str, container:BlobServiceClient, logger: logging.Logger) -> Optional[str]:
    manifest = load_manifest(create_blob_client(container, MANIFEST_BLOB_NAME), logger)
    file_data = manifest.get(source_id)
    version = file_data.get("version") if file_data else None
    if version:
        logger.info("Extracted stored version %s for %s from manifest", version, source_id)
    else:
        logger.info("No stored version for %s found in manifest", source_id)
    return version

def is_newer_version(remote: Optional[str], local: Optional[str]) -> bool:
    if remote is None:
        return False
    if local is None:
        return True
    return remote.strip() != local.strip()

def current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")

def update_manifest(
    container: ContainerClient,
    source_id: str,
    version: str,
    update_ts: str,
    hosts: list[str],
    list_of_files: list[str],
    logger: logging.Logger,
) -> None:

    manifest_client = create_blob_client(container, MANIFEST_BLOB_NAME)
    manifest = load_manifest(manifest_client, logger)

    manifest[source_id] = {
        "version": version,
        "update_ts": update_ts,
        "hosts": hosts,
        "list_of_files": list_of_files,
        "extracted": False
    }

    payload = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_client.upload_blob(payload, overwrite=True)

    logger.info(
        "Updated manifest for source '%s' (version=%s, files=%d)",
        source_id,
        version,
        len(list_of_files),
    )    

def update_latest_folder(
    source_id: str,
    container: ContainerClient,
    version: str,
    logger: logging.Logger,
) -> None:
    """
    Move all blobs in raw/{source_id}/latest/{old_version}/
    to raw/{source_id}/releases/{old_version}/,
    keeping only raw/{source_id}/latest/{version}/.
    """

    latest_prefix = f"raw/{source_id}/latest/"
    blobs = list(container.list_blobs(name_starts_with=latest_prefix))

    moved_count = 0

    for blob in blobs:
        name = blob.name  # full blob path

        # Expected:
        # raw/{source_id}/latest/{blob_version}/path/to/file
        parts = name.split("/")

        # Safety check
        if len(parts) < 5:
            continue

        blob_version = parts[3]  # after raw/{source_id}/latest/
        print(blob_version)

        # Keep current version
        if blob_version == version:
            continue

        # Compute relative path inside the version folder
        relative_path = "/".join(parts[4:])
        print(relative_path)
        release_blob_name = (
            f"raw/{source_id}/releases/{blob_version}/{relative_path}"
        )

        logger.info("Moving old version: %s -> %s", name, release_blob_name)

        src_blob = container.get_blob_client(name)
        dst_blob = container.get_blob_client(release_blob_name)

        dst_blob.start_copy_from_url(src_blob.url)
        src_blob.delete_blob()

        moved_count += 1
    
    logger.info(
        "Folder update complete — moved %d blob(s) to releases.",
        moved_count,
    )