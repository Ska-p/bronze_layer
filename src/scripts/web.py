import argparse
import yaml
import requests
import sys
import logging
import hashlib

from typing import Callable, Dict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from uuid import uuid4

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

# =========================
# Paths & imports
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]   # /app/src
CONFIG_PATH = BASE_DIR.parent / "config" / "sources.yaml"

ROOT_DIR = Path(__file__).resolve().parents[2]  # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.versioning import (
    extract_version,
    update_manifest,
    is_newer_version,
    update_latest_folder
)

from env.config import (
    BRONZE_CONTAINER,
    BLOB_CONNECTION_STRING
)

from utils.extractor import (
    extract
)

from utils.page_utils import (
    HPA_parse_version_from_page,
    MarkerDB_parse_version_from_page,
    FooDB_parse_version_from_page,
    DrugCentral_parse_version_from_page,
    TIGA_parse_version_from_page,
    ChEMBL_parse_version_from_page,
    GWASCATALOG_parse_version_from_page,
    ClinVar_parse_version_from_page,
    ChEBI_SQL_parse_version_from_page,
    OpenTargets_parse_version_from_page,
    UniProt_parse_version_from_page
)

# =========================
# Version registry
# =========================

VERSION_FUNC_REGISTRY: Dict[str, Callable[[str, str, logging.Logger], str]] = {
    "HPA": HPA_parse_version_from_page,
    "MarkerDB": MarkerDB_parse_version_from_page,
    "FooDB": FooDB_parse_version_from_page,
    "DrugCentral": DrugCentral_parse_version_from_page,
    "TIGA": TIGA_parse_version_from_page,
    "ChEMBLdb": ChEMBL_parse_version_from_page,
    "gwas": GWASCATALOG_parse_version_from_page,
    "clinvar": ClinVar_parse_version_from_page,
    "ChEBI_SQL": ChEBI_SQL_parse_version_from_page,
    "OpenTargets": OpenTargets_parse_version_from_page,
    "UniProt": UniProt_parse_version_from_page
}

# =========================
# Logging
# =========================

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("WEB")

# =========================
# Azure Blob
# =========================

blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container = blob_service.get_container_client(BRONZE_CONTAINER)

# =========================
# HTTP Session (Level-1 improvement)
# =========================

session = requests.Session()
session.headers.update({
    "User-Agent": "bronze-layer-ingestion/1.0"
})

CHUNK_SIZE = 8 * 1024 * 1024

# =========================
# Helpers
# =========================

def matches_rules(url: str, rules: dict) -> bool:
    filename = Path(urlparse(url).path).name.lower()
    if not filename:
        return False

    if "extensions" in rules:
        if not any(filename.endswith(f".{ext}") for ext in rules["extensions"]):
            return False

    if "name_contains" in rules:
        tokens = [t.lower() for t in rules["name_contains"]]
        
        # Only apply name filtering if tokens list is not empty
        if tokens:
            mode = rules.get("name_contains_mode", "or").lower()

            if mode == "and":
                if not any(token in filename for token in tokens) and not any(token in url for token in tokens):
                    return False

            elif mode == "or":
                if not any(token in filename for token in tokens) and not any(token in url for token in tokens):
                    return False

            else:
                raise ValueError(
                    f"Invalid name_contains_mode '{mode}'. "
                    "Expected 'and' or 'or'."
                )

    return True

def calculate_block_size(content_length: int | None) -> int:
    """
    Calculate optimal block size to stay under 50,000 block limit.
    
    Args:
        content_length: File size in bytes (from Content-Length header)
        
    Returns:
        Block size in bytes
    """
    MIN_BLOCK_SIZE = 4 * 1024 * 1024      # 4 MiB
    MAX_BLOCK_SIZE = 100 * 1024 * 1024    # 100 MiB (conservative)
    MAX_BLOCKS = 50_000
    
    if content_length is None:
        return 16 * 1024 * 1024  # 16 MiB
    
    # Calculate minimum block size needed
    required_block_size = content_length / MAX_BLOCKS
    
    # Round up to nearest MiB
    block_size = max(MIN_BLOCK_SIZE, int(required_block_size) + 1024 * 1024)
    
    # Cap at maximum
    return min(MAX_BLOCK_SIZE, block_size)


def stream_to_blob_large(
    url: str, 
    blob_name: str, 
    timeout: int = 600
) -> None:
    """
    Memory-efficient upload with cleanup and block limit handling.
    
    Strategy:
    1. Delete existing blob (removes all uncommitted blocks)
    2. Determine optimal block size from Content-Length
    3. Upload in chunks
    4. Clean up on failure
    """
    blob_client = container.get_blob_client(blob_name)
    block_ids = []
    
    try:
        # STEP 1: Clean slate - remove existing blob and orphaned blocks
        try:
            blob_client.delete_blob()
            logger.info("Deleted existing blob (clearing orphaned blocks): %s", blob_name)
        except ResourceNotFoundError:
            logger.debug("No existing blob to delete: %s", blob_name)
        
        # STEP 2: Start streaming and determine block size
        logger.info("Starting download: %s", url)
        
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            
            # Get file size from headers
            content_length = r.headers.get('Content-Length')
            if content_length:
                content_length = int(content_length)
                logger.info("File size: %.2f GB", content_length / (1024**3))
            
            # Calculate optimal block size
            block_size = calculate_block_size(content_length)
            max_blocks = (content_length // block_size) + 1 if content_length else "unknown"
            
            logger.info(
                "Using block size: %d MiB (estimated blocks: %s)",
                block_size // (1024 * 1024),
                max_blocks
            )
            
            # Validate block count
            if isinstance(max_blocks, int) and max_blocks > 50_000:
                raise ValueError(
                    f"File too large: would require {max_blocks} blocks "
                    f"(max 50,000). Use larger block_size or split file."
                )
            
            # STEP 3: Upload blocks
            logger.info("Uploading to blob: %s", blob_name)
            
            for chunk in r.iter_content(chunk_size=block_size):
                if chunk:
                    block_id = str(uuid4())
                    blob_client.stage_block(block_id, chunk)
                    block_ids.append(block_id)
                    
                    # Progress logging
                    if len(block_ids) % 100 == 0:
                        logger.info("Progress: %d blocks uploaded", len(block_ids))
            
            # STEP 4: Commit all blocks
            logger.info("Committing %d blocks", len(block_ids))
            blob_client.commit_block_list(block_ids)
            logger.info("✓ Upload complete: %s", blob_name)
        
    except Exception as e:
        logger.error("Upload failed for %s: %s", blob_name, e)
        
        # STEP 5: Cleanup on failure
        try:
            blob_client.delete_blob()
            logger.info("Cleaned up %d uncommitted blocks", len(block_ids))
        except Exception as cleanup_err:
            logger.warning("Cleanup failed: %s", cleanup_err)
        
        raise

def process_page(page_cfg: dict, source_id: str, version: str):
    page_url = page_cfg["web_page"]
    tag = page_cfg.get("tag", "a")
    rules = page_cfg.get("file_rules", {})

    logger.info("PAGE %s", page_url)

    r = session.get(page_url, timeout=60)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    elements = soup.find_all(tag)

    downloaded = []

    for el in elements:
        href = el.get("href")
        if not href:
            continue

        full_url = urljoin(page_url, href)
        if not matches_rules(full_url, rules):
            continue

        filename = Path(urlparse(full_url).path).name
        blob_name = f"raw/{source_id}/latest/{version}/{filename}"

        logger.info("↓ %s", filename)
        stream_to_blob_large(full_url, blob_name)

        downloaded.append(blob_name)

    return downloaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--id",
        required=True,
        help="Source id under sources.web_pages"
    )
    args = parser.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    web_sources = cfg["sources"].get("web", {})
    if args.id not in web_sources:
        raise ValueError(
            f"web_pages source '{args.id}' not found. "
            f"Available: {list(web_sources.keys())}"
        )

    source_cfg = web_sources[args.id]
    pages = source_cfg["pages"]
    version_func_name = source_cfg.get("version_func")

    if version_func_name:
        if version_func_name not in VERSION_FUNC_REGISTRY:
            raise ValueError(
                f"Unknown version_func '{version_func_name}'. "
                f"Available: {list(VERSION_FUNC_REGISTRY.keys())}"
            )

        version_func = VERSION_FUNC_REGISTRY[version_func_name]
        
        # Get filename from config or use empty string
        first_page = source_cfg["pages"][0]
        file_rules = first_page.get("file_rules", {})
        name_contains = file_rules.get("name_contains", [])
        filename = name_contains[0] if name_contains else ""
        
        version = version_func(
            first_page["web_page"],
            filename,
            logger
        )
    else:
        version = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    stored_version = extract_version(args.id, container, logger)
    if not is_newer_version(remote=version, local=stored_version):
        logger.info("%s already up to date.", args.id)
        sys.exit(0)

    logger.info("New version detected for %s: %s", args.id, version)

    all_files = []

    for page in pages:
        files = process_page(page, args.id, version)
        all_files.extend(files)

    if not all_files:
        logger.warning("No files downloaded for %s", args.id)
        return

    update_manifest(
        container=container,
        source_id=args.id,
        version=version,
        update_ts=datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S"),
        hosts=[p["web_page"] for p in pages],
        list_of_files=all_files,
        logger=logger
    )

    update_latest_folder(
        container=container,
        source_id=args.id,
        version=version,
        logger=logger
    )

if __name__ == "__main__":
    main()