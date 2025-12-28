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

from azure.storage.blob import BlobServiceClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from utils.versioning import (
    extract_version,
    update_manifest,
    is_newer_version,
    update_latest_folder
)

from config.config import (
    BRONZE_CONTAINER,
    BLOB_CONNECTION_STRING
)

from utils.page_utils import (
    HPA_parse_version_from_page,
    MarkerDB_parse_version_from_page,
    FooDB_parse_version_from_page,
    DrugCentral_parse_version_from_page,
    TIGA_parse_version_from_page
)

VERSION_FUNC_REGISTRY: Dict[str, Callable[[str, logging.Logger], str]] = {
    "HPA": HPA_parse_version_from_page,
    "MarkerDB" : MarkerDB_parse_version_from_page,
    "FooDB": FooDB_parse_version_from_page,
    "DrugCentral": DrugCentral_parse_version_from_page,
    "TIGA": TIGA_parse_version_from_page
}

CONFIG_PATH = Path("../config/sources.yaml")

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("WEB")

# =========================
# Azure Blob
# =========================
blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container = blob_service.get_container_client(BRONZE_CONTAINER)

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
        mode = rules.get("name_contains_mode", "or").lower()

        if mode == "and":
            if not all(token in filename for token in tokens):
                return False

        elif mode == "or":
            if not any(token in filename for token in tokens):
                return False

        else:
            raise ValueError(
                f"Invalid name_contains_mode '{mode}'. "
                "Expected 'and' or 'or'."
            )

    return True

def stream_to_blob(url: str, blob_name: str) -> str:
    """
    Stream HTTP content directly into Azure Blob.
    Returns SHA256 hash.
    """
    blob_client = container.get_blob_client(blob_name)
    sha256 = hashlib.sha256()

    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()

        def gen():
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    sha256.update(chunk)
                    yield chunk

        blob_client.upload_blob(
            data=gen(),
            overwrite=True,
            max_concurrency=4
        )

    return sha256.hexdigest()


def process_page(page_cfg: dict, source_id: str, version: str):
    page_url = page_cfg["web_page"]
    tag = page_cfg.get("tag", "a")
    rules = page_cfg.get("file_rules", {})

    logger.info("PAGE %s", page_url)

    r = requests.get(page_url, timeout=60)
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
        file_hash = stream_to_blob(full_url, blob_name)

        downloaded.append(blob_name)

    return downloaded

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-id",
        "--source_id",
        required=True,
        help="Source id under sources.web_pages"
    )
    args = parser.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    web_sources = cfg["sources"].get("web_pages", {})    
    source_cfg = web_sources[args.source_id]
    pages = source_cfg.get("pages", [])
    version_func_name = source_cfg.get("version_func")
    
    if args.source_id not in web_sources:
        raise ValueError(
            f"web_pages source '{args.source_id}' not found. "
            f"Available: {list(web_sources.keys())}"
        )

    if version_func_name:
        if version_func_name not in VERSION_FUNC_REGISTRY:
            raise ValueError(
                f"Unknown version_func '{version_func_name}'. "
                f"Available: {list(VERSION_FUNC_REGISTRY.keys())}"
            )

        version_func = VERSION_FUNC_REGISTRY[version_func_name]

        version = version_func(
            source_cfg["pages"][0]["web_page"],
            logger
        )
    else:
        version = datetime.now(timezone.utc).strftime("%Y-%m-%d")    
        
    stored_version = extract_version(args.source_id, container, logger)
    if not is_newer_version(remote=version, local=stored_version):
        logger.info("%s already up to date.", args.source_id)
        return
    

    logger.info("New version detected for %s: %s", args.source_id, version)

    all_files = []

    for page in pages:
        files = process_page(page, args.source_id, version)
        all_files.extend(files)

    if not all_files:
        logger.warning("No files downloaded for %s", args.source_id)
        return

    update_manifest(
        container=container,
        source_id=args.source_id,
        version=version,
        update_ts=datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S"),
        hosts=[p["web_page"] for p in pages],
        list_of_files=all_files,
        logger=logger
    )

    update_latest_folder(
        container=container,
        source_id=args.source_id,
        version=version,
        logger=logger
    )
    
if __name__ == "__main__":
    main()
