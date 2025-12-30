import argparse
import io
import gzip
import zipfile
import logging
import json
import sys
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContainerClient


# Add project root for imports
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from env.config import BLOB_CONNECTION_STRING, BRONZE_CONTAINER
from utils.data import load_manifest, create_blob_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ExtractorWorker")

def archive_root_from_blob_path(blob_path: str) -> str:
    """
    Converts:
      raw/foo/bar/archive.zip
    into:
      extracted/foo/bar/archive
    """
    if not blob_path.startswith("raw/"):
        raise ValueError(f"Unexpected blob path: {blob_path}")

    base = blob_path.replace("raw/", "extracted/", 1)
    return base.rsplit(".", 1)[0]  # remove .zip


def stream_gzip_decompression(container, raw_path, dest_path):
    blob_client = container.get_blob_client(raw_path)
    downloader = blob_client.download_blob()

    logger.info(f"Streaming decompression (GZIP): {raw_path} -> {dest_path}")

    def decompressed_chunks(chunk_size=1024 * 1024):
        with gzip.GzipFile(fileobj=downloader) as gz:
            while True:
                chunk = gz.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    container.upload_blob(dest_path, decompressed_chunks(), overwrite=True)

def stream_zip_extraction(container, raw_path):
    """
    Extracts ZIP members while preserving:
    - original blob directory
    - archive folder name
    - internal ZIP structure
    """
    print(raw_path)
    blob_client = container.get_blob_client(raw_path)
    extracted_paths = []

    archive_root = archive_root_from_blob_path(raw_path)

    logger.info(f"Extracting ZIP with structure preservation: {raw_path}")
    logger.info(f"Archive root: {archive_root}")

    # ZIP requires random access → read once
    raw_data = blob_client.download_blob().readall()

    with zipfile.ZipFile(io.BytesIO(raw_data)) as z:
        for member in z.infolist():
            if member.is_dir():
                continue

            dest_path = f"{archive_root}/{member.filename}"

            logger.info(f"Extracting ZIP member: {member.filename} → {dest_path}")

            with z.open(member) as member_file:
                container.upload_blob(
                    dest_path,
                    member_file,
                    overwrite=True
                )

            extracted_paths.append(dest_path)

    return extracted_paths


def extract(source_id: str, container: ContainerClient, logger: logging.Logger):
    service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = service.get_container_client(BRONZE_CONTAINER)
    manifest_client = create_blob_client(container, "manifest.json")
    
    # Load manifest for atomic update
    manifest = load_manifest(manifest_client, logger)
    entry = manifest.get(source_id)

    if not entry or not entry.get("list_of_files"):
        logger.error(f"No files found for {source_id}")
        return

    all_extracted = []
    try:
        for file_path in entry["list_of_files"]:
            logger.info(f"Processing source file: {file_path}")
            
            # 1. GZIP Streaming
            if file_path.lower().endswith(".gz") and not file_path.lower().endswith(".tar.gz"):
                dest = file_path.replace("raw/", "extracted/", 1).replace(".gz", "").replace(".GZ", "")
                stream_gzip_decompression(container, file_path, dest)
                all_extracted.append(dest)
                
            # 2. ZIP Member-by-Member
            elif file_path.lower().endswith(".zip"):
                # Check if it's actually a gzip file misnamed as .zip
                blob_client = container.get_blob_client(file_path)
                header = blob_client.download_blob(offset=0, length=2).readall()
                
                if header == b'\x1f\x8b':  # GZIP magic bytes
                    logger.warning(f"File {file_path} is GZIP despite .zip extension")
                    dest = file_path.replace("raw/", "extracted/", 1).replace(".zip", "")
                    stream_gzip_decompression(container, file_path, dest)
                    all_extracted.append(dest)
                else:
                    paths = stream_zip_extraction(container, file_path)
                    all_extracted.extend(paths)
            # 3. Fallback: Direct Copy (No decompression needed)
            else:
                dest = file_path.replace("raw/", "extracted/", 1)
                blob_client = container.get_blob_client(file_path)
                logger.info(f"Direct stream copy: {file_path} -> {dest}")
                # Pass the downloader directly to upload_blob for a pipe-like transfer
                container.upload_blob(dest, blob_client.download_blob(), overwrite=True)
                all_extracted.append(dest)
        
        # 4. Atomic Manifest Update: Only runs if the entire loop succeeds
        entry["extracted"] = True
        entry["extracted_list_of_files"] = all_extracted
        manifest_client.upload_blob(json.dumps(manifest, indent=2), overwrite=True)
        logger.info(f"Extraction completed successfully for {source_id}")
        
    except Exception as e:
        logger.error(f"Extraction process failed for {source_id}: {e}")
        sys.exit(1) # Mark the Batch task as failed