import argparse
import yaml
import logging
import sys
import io

from ftplib import FTP
from pathlib import Path
from typing import List, Dict, Iterator, Any
from datetime import datetime, timezone

from azure.storage.blob import ContainerClient, BlobServiceClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("FTP")


from config.config import (
    BLOB_CONNECTION_STRING,
    BRONZE_CONTAINER
)

from utils.versioning import (
    update_manifest_latest_version_ftp_file,
    update_ftp_folder_versioning,
    extract_ftp_version,
    is_newer_version
)

CHUNK_SIZE = 4 * 1024 * 1024 

import time
import random
from ftplib import FTP, error_temp
import socket

def connect_ftp(
    host: str,
    timeout: int = 60,
    retries: int = 5,
    base_delay: float = 2.0,
) -> FTP:
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            ftp = FTP()
            ftp.connect(host, timeout=timeout)
            ftp.login()  # anonymous
            return ftp

        except (ConnectionResetError, socket.timeout, error_temp) as exc:
            last_exc = exc
            sleep = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)

            logging.warning(
                "FTP connection to %s failed (attempt %d/%d): %s. Retrying in %.1fs",
                host,
                attempt,
                retries,
                exc,
                sleep,
            )
            time.sleep(sleep)

    raise RuntimeError(f"FTP connection to {host} failed after {retries} retries") from last_exc

def ftp_stream_to_blob(
    host: str,
    path: str,
    filename: str,
    container_client: ContainerClient,
    blob_path: str,
    overwrite: bool = True,
) -> None:
    logger.info("Streaming FTP file %s from %s%s", filename, host, path)

    ftp = connect_ftp(host)
    blob_client = container_client.get_blob_client(blob=blob_path)

    try:
        ftp.cwd(path)
        ftp.sendcmd("TYPE I")

        class BlobWriter(io.RawIOBase):
            def write(self, b: bytes) -> int:
                blob_client.upload_blob(
                    data=b,
                    overwrite=overwrite,
                    length=len(b),
                    max_concurrency=1,
                )
                return len(b)

        writer = BlobWriter()

        ftp.retrbinary(
            f"RETR {filename}",
            callback=writer.write,
            blocksize=CHUNK_SIZE,
        )

        logger.info("Uploaded FTP file to blob %s", blob_client.blob_name)

    finally:
        try:
            ftp.quit()
        except Exception:
            ftp.close()
            
def split_host_and_path(host_with_path: str) -> tuple[str, str]:
    parts = host_with_path.split("/", 1)
    host = parts[0]
    path = "/" + parts[1] if len(parts) > 1 else "/"
    return host, path

def get_ftp_last_modified(host: str, path: str, filename: str) -> str | None:
    ftp = connect_ftp(host)
    ftp.cwd(path)
    try:
        response = ftp.sendcmd(f"MDTM {filename}")
        timestamp = response.split()[1]
        last_modified = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        return last_modified.strftime("%Y%m%d%H%M%S")
    except Exception as e:
        print(e)
        return None
    finally:
        ftp.quit()

def matches_rules(
    filename: str,
    extensions: list[str] | None = None,
    name_contains: list[str] | None = None,
    exclude: list[str] | None = None,
) -> bool:
    if exclude and any(token in filename for token in exclude):
        return False

    if extensions and not any(filename.endswith(f".{ext}") for ext in extensions):
        return False

    if name_contains and not any(token in filename for token in name_contains):
        return False

    return True

def list_ftp_files(host: str, path: str) -> list[str]:
    ftp = connect_ftp(host)
    ftp.cwd(path)
    files = ftp.nlst()
    ftp.quit()
    return files

def run_ftp_source(source_id: str, source_cfg: Dict) -> None:
    logger.info("Starting %s data synchronization workflow.", source_id.upper())

    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_client.get_container_client(BRONZE_CONTAINER)

    update_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")

    all_blob_paths: list[str] = []
    used_hosts: set[str] = set()
    detected_versions: set[str] = set()

    for host_cfg in source_cfg:
        host_with_path = host_cfg["host"]
        file_rules = host_cfg.get("file_rules", {})

        extensions = file_rules.get("extensions")
        name_contains = file_rules.get("name_contains")
        exclude = file_rules.get("exclude")
        root = file_rules.get("root")
        root_prefix = root.strip("/") + "/" if root else ""
        
        host, path = split_host_and_path(host_with_path)
        used_hosts.add(host_with_path.rstrip("/"))

        logger.info("Connecting to FTP host: %s", host)
        logger.info("Analyzing folder: %s", path)

        try:
            filenames = list_ftp_files(host, path)
        except Exception as e:
            logger.error("Failed to list files on %s%s: %s", host, path, e)
            continue

        matching_files = [
            f for f in filenames
            if matches_rules(
                f,
                extensions=extensions,
                name_contains=name_contains,
                exclude=exclude,
            )
        ]

        if not matching_files:
            logger.info("No matching files found in %s%s", host, path)
            continue

        for filename in matching_files:
            last_modified_ts = get_ftp_last_modified(host, path, filename)
            if not last_modified_ts:
                logger.warning("Skipping %s (cannot determine last-modified)", filename)
                continue

            version = datetime.strptime(
                last_modified_ts, "%Y%m%d%H%M%S"
            ).strftime("%Y-%m-%d")

            detected_versions.add(version)
            
            stored_ts = extract_ftp_version(source_id, container, logger)
            if not is_newer_version(version, stored_ts):
                logger.info("%s up to date.", filename)
                continue

            blob_name = (
                f"raw/{source_id}/latest/{version}/"
                f"{root_prefix}{filename}"
            )
            
            ftp_stream_to_blob(
                host=host,
                path=path,
                filename=filename,
                container_client=container,
                blob_path=blob_name,
            )

            all_blob_paths.append(blob_name)

    if not detected_versions:
        logger.info("No updates detected for source %s", source_id)
        return

    # Resolve single authoritative version
    version = sorted(detected_versions)[-1]

    update_manifest_latest_version_ftp_file(
        container=container,
        source_id=source_id,
        version=version,
        update_ts=update_ts,
        hosts=sorted(used_hosts),
        list_of_files=all_blob_paths,
        logger=logger,
    )

def load_sources_config(config_path: Path) -> Dict[str, Dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(f"Sources configuration not found at {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle) or {}
    ftp_sources = document["sources"]["ftp"]
    if not isinstance(ftp_sources, dict):
        raise ValueError("The 'sources' section in sources.yaml must be a mapping.")
    return ftp_sources

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline with a given identifier")
    parser.add_argument(
        "-id",
        "--id",
        required=True,
        help="Identifier to select configuration/template"
    )
    args = parser.parse_args()
    id = args.id
    sources = load_sources_config(Path("../config/sources.yaml"))
    print(sources)
    source_config = sources[id]
    run_ftp_source(id, source_config) # type: ignore