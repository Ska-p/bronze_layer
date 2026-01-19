import requests
import json
import time
import sys
import logging

from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from azure.storage.blob import BlobServiceClient, ContainerClient

# ================= PATH SETUP =================
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR.parent / "config" / "sources.yaml"

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

OUTPUT_FILE = "icd_10_entities.json"

# ================= PROJECT IMPORTS =================
from utils.versioning import (
    extract_version,
    update_manifest,
    is_newer_version,
    update_latest_folder
)

from env.config import (
    BRONZE_CONTAINER,
    BLOB_CONNECTION_STRING,
    ICD_10_RELEASE_INDEX_URL,
    ICD_CLIENT_ID,
    ICD_SCOPE,
    ICD_CLIENT_SECRET,
    ICD_TOKEN_ENDPOINT,
)

# ================= CONFIG =================
access_token = None
access_token_expires_at = 0

# Logger
logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("ICD-10")

# ================= BLOB =================
blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container: ContainerClient = blob_service.get_container_client(BRONZE_CONTAINER)

# ================= HELPERS =================
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


def upload_json_to_blob(
    container: ContainerClient,
    local_path: str,
    blob_path: str,
):
    blob_client = container.get_blob_client(blob_path)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    logger.info("Uploaded %s to blob path: %s", local_path, blob_path)

# ================= AUTH =================
def fetch_token():
    global access_token, access_token_expires_at

    resp = requests.post(
        ICD_TOKEN_ENDPOINT, # type: ignore
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": ICD_CLIENT_ID,
            "client_secret": ICD_CLIENT_SECRET,
            "scope": ICD_SCOPE
        },
        timeout=10
    )
    resp.raise_for_status()

    payload = resp.json()
    access_token = payload["access_token"]

    expires_in = payload.get("expires_in", 3600)
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

# ================= START =================
logger.info("START")

logger.info("Fetching initial token")
fetch_token()

# ================= GET LATEST RELEASE =================
logger.info("GET LATEST RELEASE INDEX")

release_index_resp = requests.get(ICD_10_RELEASE_INDEX_URL, headers=get_headers(), timeout=10) # type: ignore
release_index_resp.raise_for_status()
release_index = release_index_resp.json()

latest_release_url = release_index["latestRelease"]
logger.info("Latest release URL: %s", latest_release_url)

# ================= CHECK VERSION =================
logger.info("CHECK VERSION")

latest_release_resp = requests.get(
    latest_release_url.replace("http://", "https://"),
    headers=get_headers(),
    timeout=10
)
latest_release_resp.raise_for_status()
latest_release_json = latest_release_resp.json()

release_date = latest_release_json.get("releaseDate")
logger.info("Remote release date: %s", release_date)

stored_version = extract_version("icd10", container, logger)

if not is_newer_version(remote=release_date, local=stored_version):
    logger.info("ICD-10 already up to date. Exiting.")
    sys.exit(0)

logger.info("New ICD-10 version detected")

# ================= RECURSIVE DOWNLOAD =================
logger.info("RECURSIVE DOWNLOAD")

entities_to_query = deque()
visited = set()
results = []

for child in latest_release_json.get("child", []):
    entities_to_query.append(child)

while entities_to_query:
    entity_url = entities_to_query.popleft()

    if entity_url in visited:
        continue
    visited.add(entity_url)

    resp = requests.get(
        entity_url.replace("http://", "https://"),
        headers=get_headers(),
        timeout=10
    )

    if resp.status_code == 401:
        fetch_token()
        resp = requests.get(
            entity_url.replace("http://", "https://"),
            headers=get_headers(),
            timeout=10
        )

    if not resp.ok:
        logger.warning("Skipped %s (%s)", entity_url, resp.status_code)
        continue

    raw = resp.json()
    normalized = strip_at_keys(raw)
    results.append(normalized)

    for child in raw.get("child", []):
        if child not in visited:
            entities_to_query.append(child)

    title = normalized.get("title", {}).get("value") # type: ignore
    entity_id = normalized.get("id") # type: ignore
    logger.info("Fetched %s (ID: %s)", title, entity_id)

# ================= OUTPUT =================
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

logger.info("Local JSON written: %s", OUTPUT_FILE)

blob_path = f"raw/icd10/latest/{release_date}/{OUTPUT_FILE}"

upload_json_to_blob(
    container=container,
    local_path=OUTPUT_FILE,
    blob_path=blob_path
)

# Update manifest & latest folder
update_manifest(
    source_id="icd10",
    version=release_date,
    container=container,
    logger=logger,
    update_ts=datetime.now(timezone.utc).strftime(format="%Y-%m-%d_%H-%M-%s"),
    hosts = [ICD_10_RELEASE_INDEX_URL], # type: ignore
    list_of_files=[blob_path]
)

update_latest_folder(
    source_id="icd10",
    version=release_date,
    container=container,
    logger=logger
)

logger.info("DONE — %d entities written and uploaded", len(results))