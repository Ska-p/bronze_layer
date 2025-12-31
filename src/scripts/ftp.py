import argparse
import yaml
import logging
import sys
import time
import random
import socket

from ftplib import FTP, error_temp
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from azure.storage.blob import ContainerClient, BlobServiceClient

BASE_DIR = Path(__file__).resolve().parents[1]   # /app/src
CONFIG_PATH = BASE_DIR.parent / "config" / "sources.yaml"

ROOT_DIR = Path(__file__).resolve().parents[2]  # bronze_layer/
SRC_DIR = ROOT_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("FTP")

from env.config import (
    BLOB_CONNECTION_STRING,
    BRONZE_CONTAINER
)

from utils.versioning import (
    update_manifest,
    update_latest_folder,
    extract_version,
    is_newer_version
)

from extractor import (
    extract
)

def connect_ftp(
    host: str,
    timeout: int = 3600,  # 1 hour instead of 5 minutes
    retries: int = 5,
    base_delay: float = 2.0,
) -> FTP:
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            ftp = FTP()
            ftp.connect(host, timeout=timeout)
            ftp.login()  # anonymous
            if ftp.sock is not None:
                ftp.sock.settimeout(timeout)            
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
) -> None:
    logger.info("Streaming FTP file %s from %s%s", filename, host, path)

    ftp = connect_ftp(host, timeout=3600*24)
    blob_client = container_client.get_blob_client(blob_path)
    data_sock = None
    
    try:
        # Delete existing blob
        try:
            blob_client.delete_blob()
        except Exception:
            pass

        # Configure FTP
        ftp.set_pasv(True)
        ftp.cwd(path)
        
        # Open data connection
        ftp.sendcmd("TYPE I")
        data_sock = ftp.transfercmd(f"RETR {filename}")
        data_sock.settimeout(3600)
        
        logger.info("Starting upload to Azure (this may take 30+ minutes for large files)...")
        
        # Stream to Azure
        with data_sock.makefile("rb") as fp:
            blob_client.upload_blob(
                data=fp,
                overwrite=True,
                blob_type="BlockBlob",
                max_concurrency=4                         
            )
        
        # Close data socket - upload succeeded if we reach here
        data_sock.close()
        data_sock = None
        
        logger.info("✓ Uploaded FTP file to blob %s", blob_path)
                
    except Exception as e:
        logger.error("Failed to stream %s: %s", filename, e)
        raise
        
    finally:
        # Cleanup
        if data_sock:
            try:
                data_sock.close()
            except Exception:
                pass
        
        try:
            ftp.quit()
        except Exception:
            try:
                ftp.close()
            except Exception:
                pass
            
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
            
            stored_ts = extract_version(source_id, container, logger)
            if not is_newer_version(version, stored_ts):
                logger.info("%s up to date.", filename)
                sys.exit(0)

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

    update_manifest(
        container=container,
        source_id=source_id,
        version=version,
        update_ts=update_ts,
        hosts=sorted(used_hosts),
        list_of_files=all_blob_paths,
        logger=logger,
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
    
    # Extract if file compressed

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
        "--id",
        required=True,
        help="Identifier to select configuration/template"
    )
    args = parser.parse_args()
    id = args.id
    sources = load_sources_config(Path(CONFIG_PATH))
    print(sources)
    source_config = sources[id]
    run_ftp_source(id, source_config) # type: ignore