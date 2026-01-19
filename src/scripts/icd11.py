import requests
import json
import time
import sys
import logging

from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from azure.storage.blob import BlobServiceClient, ContainerClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parents[1]   
CONFIG_PATH = BASE_DIR.parent / "config" / "sources.yaml"

ROOT_DIR = Path(__file__).resolve().parents[2] 
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
    BLOB_CONNECTION_STRING,
    ICD_11_ROOT_URL,
    ICD_CLIENT_ID,
    ICD_CLIENT_SECRET,
    ICD_SCOPE,
    ICD_TOKEN_ENDPOINT
)

# ================= CONFIG =================
OUTPUT_FILE = "icd_11_entities.json"
access_token = None
access_token_expires_at = 0
# Blob
blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container = blob_service.get_container_client(BRONZE_CONTAINER)
# Logger
logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("ICD-11")

# ================= HELPERS =================
def extract_id(uri: str) -> str:
    return uri.rstrip("/").split("/")[-1]

def strip_at_keys(obj):
    """
    Recursively remove leading '@' from dictionary keys
    while preserving the original structure and values.
    """
    if isinstance(obj, dict):
        return {
            (k[1:] if isinstance(k, str) and k.startswith("@") else k): strip_at_keys(v)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [strip_at_keys(item) for item in obj]
    else:
        return obj

def create_session():
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )

    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = create_session()

def upload_json_to_blob(
    container: ContainerClient,
    local_path: str,
    blob_path: str,
    logger: logging.Logger
):
    blob_client = container.get_blob_client(blob_path)

    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    logger.info("Uploaded %s to blob path: %s", local_path, blob_path)

# ================= START =================
logger.info("START")

# ================= SETUP =================
logger.info("SETUP: fetching token")

def fetch_token():
    global access_token, access_token_expires_at

    resp = session.post(
        ICD_TOKEN_ENDPOINT, # type: ignore
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": ICD_CLIENT_ID,
            "client_secret": ICD_CLIENT_SECRET,
            "scope": ICD_SCOPE
        },
        timeout=(5, 20)
    )
    resp.raise_for_status()

    payload = resp.json()
    access_token = payload["access_token"]

    # expires_in is in seconds (~3600)
    expires_in = payload.get("expires_in", 3600)

    # refresh 60 seconds before expiry
    access_token_expires_at = time.time() + expires_in - 60

    logger.info("Token refreshed")

def get_headers():
    if time.time() >= access_token_expires_at:
        fetch_token()

    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Accept-Language": "en",
        "API-Version": "v2"
    }

fetch_token()

# ================= CHECK VERSION =================
logger.info("CHECK VERSION")

root_resp = session.get(ICD_11_ROOT_URL, headers=get_headers(), timeout=(5, 20)) # type: ignore
root_resp.raise_for_status()
root_json = root_resp.json()
release_date = root_json.get("releaseDate")
stored_version = extract_version("icd11", container, logger)
# Check Version
if not is_newer_version(remote=release_date, local=stored_version):
    logger.info("ICD-11 already up to date.")
    exit(0)
    
logger.info("New version detected for ICD-11: %s", release_date)

# ================= RECURSIVE DOWNLOAD =================
logger.info("RECURSIVE DOWNLOAD")

entities_to_query = deque()
visited = set()
results = []

# seed with root entity children (foundation)
for child in root_json.get("child", []):
    entities_to_query.append(child)

while entities_to_query:
    entity_url = entities_to_query.popleft()

    if entity_url in visited:
        continue
    visited.add(entity_url)
    
    try:
        resp = session.get(entity_url.replace("http://", "https://"), headers=get_headers(), timeout=(5, 20))
    except requests.exceptions.RequestException as e:
        logger.warning("Connection error, skipping %s: %s", entity_url, e)
        continue
    
    resp.raise_for_status()
    raw = resp.json()
    normalized = strip_at_keys(raw)
    results.append(normalized)

    for child in raw.get("child", []):
        if child not in visited:
            entities_to_query.append(child)

    title = normalized.get("title", {}).get("value")
    entity_id = normalized.get("id")
    logger.info("Fetched %s (ID: %s)", title, entity_id)

# ================= OUTPUT =================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

logger.info(f"DONE — {len(results)} entities written to {OUTPUT_FILE}")

# Blob path convention (adjust if needed)
blob_path = f"raw/icd11/latest/{release_date}/{OUTPUT_FILE}"

upload_json_to_blob(
    container=container,
    local_path=OUTPUT_FILE,
    blob_path=blob_path,
    logger=logger
)

# Update manifest & latest folder
update_manifest(
    source_id="icd11",
    version=release_date,
    container=container,
    logger=logger,
    update_ts=datetime.now(timezone.utc).strftime(format="%Y-%m-%d_%H-%M-%s"),
    hosts = [ICD_11_ROOT_URL], # type: ignore
    list_of_files=[blob_path]
)

update_latest_folder(
    source_id="icd11",
    version=release_date,
    container=container,
    logger=logger
)

logger.info("DONE — %d entities written and uploaded", len(results))