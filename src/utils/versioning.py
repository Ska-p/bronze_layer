import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import json

from azure.storage.blob import ContainerClient, BlobClient, BlobServiceClient

from utils.data import (
    load_manifest,
    create_blob_client,
    get_newest_blob,
    copy_blob_within_container,
)

ONTOLOGY_BASE_PREFIX = "raw/ontology_folders"
LATEST_FOLDER = "latest"
RELEASE_FOLDER = "release"
DEFAULT_EXTENSION = ".owl"
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

def extract_ols_version(ontology_id: str, container: BlobServiceClient, logger: logging.Logger) -> Optional[str]:
    manifest = load_manifest(create_blob_client(container, MANIFEST_BLOB_NAME), logger)
    file_data = manifest.get(ontology_id)
    version = file_data.get("version") if file_data else None
    if version:
        logger.info("Extracted stored version %s for %s from manifest", version, ontology_id)
    else:
        logger.info("No stored version for %s found in manifest", ontology_id)
    return version

def extract_ftp_version(source_id:str, container:BlobServiceClient, logger: logging.Logger) -> Optional[str]:
    manifest = load_manifest(create_blob_client(container, MANIFEST_BLOB_NAME), logger)
    file_data = manifest.get(source_id)
    version = file_data.get("version") if file_data else None
    if version:
        logger.info("Extracted stored version %s for %s from manifest", version, source_id)
    else:
        logger.info("No stored version for %s found in manifest", source_id)
    return version


def extract_web_version(source_id:str, container:BlobServiceClient, filename: str, logger: logging.Logger) -> Optional[str]:
    manifest = load_manifest(create_blob_client(container, MANIFEST_BLOB_NAME), logger)
    file_data = manifest.get(filename)
    version = file_data.get(file_data) if file_data else None
    if version:
        logger.info("Extracted stored version %s for %s from manifest", version, source_id)
    else:
        logger.info("No stored version for %s found in manifest", source_id)
    return version

def extract_latest_version(manifest: dict, ontology_id: str) -> str | None :
    entry = manifest.get(ontology_id)
    return entry.get("version") if entry else None

def is_newer_version(remote: Optional[str], local: Optional[str]) -> bool:
    if remote is None:
        return False
    if local is None:
        return True
    return remote.strip() != local.strip()

def current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")

def _build_blob_key(folder: str, filename: str) -> str:
    return f"{ONTOLOGY_BASE_PREFIX}/{folder}/{filename}"

def _normalize_suffix(suffix: Optional[str]) -> str:
    if not suffix:
        return DEFAULT_EXTENSION
    if suffix.startswith("."):
        return suffix
    return f".{suffix}"

def build_latest_blob_name(timestamp: str, suffix: Optional[str] = None) -> str:
    normalized_suffix = _normalize_suffix(suffix)
    filename = f"{timestamp}_ontology_latest{normalized_suffix}"
    return _build_blob_key(LATEST_FOLDER, filename)

def build_release_blob_name(timestamp: str, suffix: Optional[str] = None) -> str:
    normalized_suffix = _normalize_suffix(suffix)
    filename = f"{timestamp}_ontology_release{normalized_suffix}"
    return _build_blob_key(RELEASE_FOLDER, filename)

def _extract_suffix_from_blob_name(blob_name: str) -> str:
    suffix = "".join(Path(blob_name).suffixes)
    return suffix if suffix else DEFAULT_EXTENSION

def archive_existing_latest_blob(container: ContainerClient, timestamp: str, logger: logging.Logger) -> Optional[str]:
    existing_blob = get_newest_blob(container, _build_blob_key(LATEST_FOLDER, ""), logger)
    if existing_blob is None:
        logger.info("No existing latest blob to archive for timestamp %s", timestamp)
        return None
    suffix = _extract_suffix_from_blob_name(existing_blob.name)
    release_blob_name = build_release_blob_name(timestamp, suffix)
    copy_blob_within_container(container, existing_blob.name, release_blob_name, logger)
    logger.info("Archived blob %s into %s", existing_blob.name, release_blob_name)
    return release_blob_name

def update_manifest_latest_version_ols(
    container: ContainerClient,
    source_id: str,
    filename: str,
    version: str,
    last_updated_ts: str,
    latest_prefix: str,
    logger: logging.Logger
) -> None:
    manifest_client = create_blob_client(container, MANIFEST_BLOB_NAME)
    manifest = load_manifest(manifest_client, logger)

    # Extract extensions safely (supports .owl.gz, .tar.gz, etc.)
    last_parts = filename.split("/")[-1]
    parts = last_parts.split(".")
    extensions = parts[1:] if len(parts) > 1 else []
    blob_path = f"{latest_prefix}/{last_parts}"
    # Update source-level entry
    manifest[source_id] = {
        "version" : version, 
        "update_ts": last_updated_ts,
        "list_of_files": [blob_path]
    }

    payload = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_client.upload_blob(payload, overwrite=True)

    logger.info(
        "Updated manifest for source '%s' using file '%s' (ts=%s)",
        source_id,
        filename,
        last_updated_ts,
    )
    
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
    }

    payload = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_client.upload_blob(payload, overwrite=True)

    logger.info(
        "Updated manifest for source '%s' (version=%s, files=%d)",
        source_id,
        version,
        len(list_of_files),
    )    
    
def update_manifest_latest_version_ftp_file(
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
    }

    payload = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_client.upload_blob(payload, overwrite=True)

    logger.info(
        "Updated manifest for source '%s' (version=%s, files=%d)",
        source_id,
        version,
        len(list_of_files),
    )

    
def update_manifest_latest_version_web_file(
    container: ContainerClient,
    source_id: str,
    filename: str,
    last_updated_ts: str,
    blob_path: str,
    update_ts: str,
    logger: logging.Logger
) -> None:
    manifest_client = create_blob_client(container, MANIFEST_BLOB_NAME)
    manifest = load_manifest(manifest_client, logger)

    # Ensure source_id level
    source_entry = manifest.setdefault(source_id, {})

    # Extract extensions safely (supports .owl.gz, .tar.gz, etc.)
    parts = filename.split(".")
    filename_no_ext = parts[0]
    extensions = parts[1:] if len(parts) > 1 else []

    # Update file-specific entry
    source_entry[filename] = {
        "last_updated_ts": last_updated_ts,
        "blob_path": blob_path,
        "extensions": extensions,
    }

    payload = json.dumps(manifest, indent=2).encode("utf-8")
    manifest_client.upload_blob(payload, overwrite=True)

    logger.info(
        "Updated manifest for source '%s', file '%s' (ts=%s)",
        source_id,
        filename,
        last_updated_ts,
    )

def update_ols_folder_versioning(container: ContainerClient, ontology_id: str, timestamp: str, logger: logging.Logger):
    """
    Move old latest version blobs into releases folder.
    raw/{ontology_id}/latest/* --> raw/{ontology_id}/releases/*

    Only moves files whose blob name does NOT include the given timestamp.
    """
    latest_prefix = f"raw/{ontology_id}/latest/"
    releases_prefix = f"raw/{ontology_id}/releases/"

    logger.info("Scanning folder: %s", latest_prefix)

    moved_count = 0

    for blob in container.list_blobs(name_starts_with=latest_prefix):
        blob_name = blob.name

        # Skip the current latest version
        if timestamp in blob_name:
            logger.debug("Keeping latest blob: %s", blob_name)
            continue

        # Extract filename only
        filename = blob_name.split("/")[-1]
        new_blob_name = f"{releases_prefix}{filename}"

        logger.info("Moving %s -> %s", blob_name, new_blob_name)

        # Copy into releases folder (server-side, efficient)
        source_url = container.get_blob_client(blob_name).url
        new_blob_client = container.get_blob_client(new_blob_name)
        new_blob_client.start_copy_from_url(source_url)

        # Delete original
        container.delete_blob(blob_name)

        logger.debug("Moved: %s -> %s", blob_name, new_blob_name)
        moved_count += 1

    logger.info("Folder update complete — moved %d old blob(s).", moved_count)
    return moved_count

def update_ftp_folder_versioning(
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

        # Keep current version
        if blob_version == version:
            continue

        # Compute relative path inside the version folder
        relative_path = "/".join(parts[4:])

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

    latest_prefix = f"raw/{source_id}/latest/{version}"
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

        # Keep current version
        if blob_version == version:
            continue

        # Compute relative path inside the version folder
        relative_path = "/".join(parts[4:])

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
    
    
def update_web_folder_versioning(
    *,
    source_id: str,
    container: ContainerClient,
    latest_ts: str,
    logger: logging.Logger
) -> None:
    """
    Move all blobs in raw/{source_id}/latest/ that do NOT match latest_ts
    into raw/{source_id}/releases/{their_ts}/
    """

    latest_prefix = f"raw/{source_id}/latest/"
    blobs = list(container.list_blobs(name_starts_with=latest_prefix))
    moved_count = 0
    for blob in blobs:
        name = blob.name  # full blob path

        # Skip placeholders or unexpected files
        filename = name.split("/")[-1]
        if "_" not in filename:
            continue

        blob_ts, original_name = filename.split("_", 1)

        # Keep newest version
        if blob_ts == latest_ts:
            continue

        # Move to releases/{blob_ts}/
        release_blob_name = (
            f"raw/{source_id}/releases/{blob_ts}/{original_name}"
        )

        if logger:
            logger.info(
                f"Moving old version: {name} -> {release_blob_name}"
            )

        # Copy then delete (Blob Storage has no rename)
        src_blob = container.get_blob_client(name)
        dst_blob = container.get_blob_client(release_blob_name)

        dst_blob.start_copy_from_url(src_blob.url)
        src_blob.delete_blob()
        logger.info("Moved: %s -> %s", name, release_blob_name)
        moved_count += 1

    logger.info("Folder update complete — moved %d old blob(s).", moved_count)