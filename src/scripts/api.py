import sys
import logging
import hashlib
import argparse
import requests
import yaml

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Callable, Optional, Any

from azure.storage.blob import BlobServiceClient


BASE_DIR = Path(__file__).resolve().parents[1]   # /app/src
CONFIG_PATH = BASE_DIR.parent / "config" / "sources.yaml"

ROOT_DIR = Path(__file__).resolve().parents[2]  # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from env.config import BLOB_CONNECTION_STRING, BRONZE_CONTAINER

from utils.versioning import (
    update_latest_folder,
    update_manifest,
    is_newer_version,
    extract_version
)

from extractor import (
    extract
)

from utils.page_utils import (
    QUICKGO_version,
    HGNC_version
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

CHUNK_SIZE = 8 * 1024 * 1024

VERSION_FUNC_REGISTRY: Dict[str, Callable[[logging.Logger], str]] = {
    "quickgo": QUICKGO_version,
    "hgnc": HGNC_version
}

def load_sources_config(config_path: Path) -> Dict[str, Dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(f"Sources configuration not found at {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle) or {}
    ftp_sources = document["sources"]["api"]
    if not isinstance(ftp_sources, dict):
        raise ValueError("The 'sources' section in sources.yaml must be a mapping.")
    return ftp_sources
    
def is_probe_operation(op: Dict) -> bool:
    return "probe" in op

def build_request(
    base_url: str,
    operation: Dict
) -> Dict:
    if "probe" in operation:
        path = operation["probe"]
    else:
        path = operation["name"]

    return {
        "url": f"{base_url.rstrip('/')}/{path}",
        "method": "GET",
        "params": operation.get("params"),
        "headers": operation.get("headers", {})
    }

def run_probe(
    source_id: str,
    source_cfg: Dict
) -> Optional[str]:

    extractor = VERSION_FUNC_REGISTRY.get(source_id)
    if not extractor:
        return None
    
    return None

def download_and_upload(
    request_cfg: Dict,
    container,
    blob_name: str
) -> str:

    blob_client = container.get_blob_client(blob_name)
    sha256 = hashlib.sha256()

    with requests.get(
        request_cfg["url"],
        params=request_cfg.get("params"),
        headers=request_cfg.get("headers"),
        stream=True,
        timeout=300
    ) as resp:

        resp.raise_for_status()

        def stream():
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    sha256.update(chunk)
                    yield chunk

        blob_client.upload_blob(stream(), overwrite=True)

    return sha256.hexdigest()

def run_ingestion(source_id: str):

    source_cfg = load_sources_config(Path(CONFIG_PATH))[source_id]

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_service.get_container_client(BRONZE_CONTAINER)

    version = run_probe(source_id, source_cfg)
    stored_version = extract_version(source_id, container, logger)
    
    if version:
        base_path = f"raw/{source_id}/latest/{version}"
        logger.info("Detected version: %s", version)
    else:
        version = datetime.now(timezone.utc).strftime("%Y_%m_%d")
        base_path = f"raw/{source_id}/latest/{version}"

    if not is_newer_version(remote=version, local=stored_version):
        logger.info("%s already up to date.", args.id)
        sys.exit(0)

    hosts = []
    list_of_files = []
    for op in source_cfg.get("operations", []):

        if is_probe_operation(op):
            continue

        req = build_request(source_cfg["base_url"], op)
        hosts.append(req["url"])
        
        blob_name = f"{base_path}/{op['filename']}"
        list_of_files.append(blob_name)
        
        logger.info("Downloading %s", req["url"])

        sha = download_and_upload(req, container, blob_name)

        logger.info(
            "Uploaded %s (SHA256=%s)",
            blob_name,
            sha
        )
    
    update_manifest(
        source_id=source_id, 
        container=container, 
        version=version,
        update_ts=datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S"),
        hosts=hosts,
        list_of_files=list_of_files,
        logger=logger
    )
    
    update_latest_folder(
        container=container,
        source_id=source_id,
        version=version,
        logger=logger
    )
    
    extract(
        source_id=source_id,
        container=container,
        logger=logger
    )

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--id",
        required=True,
        help="Source ID (e.g. quickgo, clinpgx)"
    )

    args = parser.parse_args()
    run_ingestion(args.id)
