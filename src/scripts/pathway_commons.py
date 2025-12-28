import requests
import sys
import logging

from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timezone
from typing import Tuple, Optional

from azure.storage.blob import BlobServiceClient


PC2_BASE_URL = "https://download.baderlab.org/PathwayCommons/PC2/"
TARGET_FILENAME = "pc-hgnc.txt.gz"

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    
from config.config import BLOB_CONNECTION_STRING, BRONZE_CONTAINER

from utils.versioning import (
    update_latest_folder,
    update_manifest,
    is_newer_version,
    extract_version
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

def download_latest_pc2_hgnc(
    timeout: Optional[int] = 30,
):
    """
    Determine the PC2 version whose pc-hgnc.txt.gz has the latest Last-Modified
    timestamp, then stream-download it directly to Azure Blob Storage.

    Returns:
        (selected_version, last_modified_utc)
    """

    session = requests.Session()

    # ------------------------------------------------------------------
    # 1) Discover candidate versions (HTML used only for link discovery)
    # ------------------------------------------------------------------
    resp = session.get(PC2_BASE_URL, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    versions: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip("/")
        if href.startswith("v") and href[1:].isdigit():
            versions.append(href)

    if not versions:
        raise RuntimeError("No PC2 version folders found")

    # ------------------------------------------------------------------
    # 2) Probe each version via HEAD and extract Last-Modified
    # ------------------------------------------------------------------
    best_version: Optional[str] = None
    best_last_modified: Optional[datetime] = None
    best_file_url: Optional[str] = None

    for version in versions:
        file_url = urljoin(PC2_BASE_URL, f"{version}/{TARGET_FILENAME}")

        try:
            head = session.head(file_url, allow_redirects=True, timeout=timeout)
            if head.status_code != 200:
                continue

            lm = head.headers.get("Last-Modified")
            if not lm:
                continue

            last_modified = datetime.strptime(
                lm, "%a, %d %b %Y %H:%M:%S %Z"
            ).replace(tzinfo=timezone.utc)

            if (
                best_last_modified is None
                or last_modified > best_last_modified
            ):
                best_version = version
                best_last_modified = last_modified
                best_file_url = file_url

        except requests.RequestException:
            continue
        
    date_str = best_last_modified.strftime("%Y-%m-%d") # type: ignore
    blob_path = f"raw/bronze_layer/pathway_commons/latest/{date_str}/{TARGET_FILENAME}"
    
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_client.get_container_client(BRONZE_CONTAINER)
    stored_version = extract_version("pathway_commons", 
                                     container=container,
                                     logger=logger)
    
    if is_newer_version(remote=stored_version, local=date_str):
        logging.info("Pathway Commons data up to date.")
        return
    
    blob_client = container.get_blob_client(
        blob=blob_path,
    )

    with session.get(best_file_url, stream=True, timeout=timeout) as r: # type: ignore
        r.raise_for_status()

        blob_client.upload_blob(
            data=r.iter_content(chunk_size=1024 * 1024),
            overwrite=True,
        )
    
    timestamp = datetime.now().strftime("%Y-%m-%d")
        
    update_manifest(
        container=container,
        source_id="pathway_commons",
        version=datetime.strftime(best_last_modified, "%Y-%m-%d"), # type: ignore
        update_ts=timestamp,
        hosts=[f"{PC2_BASE_URL}{best_version}"], # type: ignore
        list_of_files = [blob_path],
        logger=logger
    )
    
    update_latest_folder(
        source_id="pathway_commons",
        container=container,
        version=datetime.strftime(best_last_modified, "%Y-%m-%d"), # type: ignore
        logger=logger
    )

if __name__=="__main__":
    download_latest_pc2_hgnc()