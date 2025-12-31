import requests
import re
import logging

from datetime import datetime, timezone
from bs4 import BeautifulSoup
from typing import Dict

def HGNC_version(logger: logging.Logger) -> str: # data passed as placeholder 
    endpoint = "https://www.genenames.org/rest/info"

    logger.info("Fetching HGNC version info from %s", endpoint)

    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()

    payload = response.json()

    raw_ts = payload.get("lastModified")
    dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))

    version = dt.strftime("%Y-%m-%d")
    logger.info("HGNC lastModified resolved to version: %s", version)

    return version

def QUICKGO_version(logger: logging.Logger) -> str: # data passed as placeholder 
    endpoint = "https://www.ebi.ac.uk/QuickGO/services/annotation/about"

    logger.info("Fetching HGNC version info from %s", endpoint)

    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()

    payload = response.json()

    raw_ts = payload.get("annotation").get("timestamp")
    
    version = raw_ts.strftime("%Y-%m-%d")
    logger.info("HGNC lastModified resolved to version: %s", version)

    return version

def TIGA_parse_version_from_page(url: str, logger: logging.Logger) -> str:
    """
    Extract version from Apache directory listing by taking the
    latest 'Last modified' timestamp among .tsv files.
    """
    logger.info("Fetching TIGA version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.find_all("tr")
    if not rows:
        raise ValueError("TIGA: no rows found in directory listing.")

    timestamps = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        link = cols[1].find("a")
        if not link:
            continue

        filename = link.get_text(strip=True)

        # Only consider TSV files
        if not filename.lower().endswith(".tsv"):
            continue

        raw_ts = cols[2].get_text(strip=True)
        if not raw_ts:
            continue

        try:
            # Format: YYYY-MM-DD HH:MM
            dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M")
            timestamps.append(dt)
        except ValueError:
            logger.warning("TIGA: skipping unparsable timestamp '%s'", raw_ts)

    if not timestamps:
        raise ValueError("TIGA: no valid TSV timestamps found.")

    latest = max(timestamps)
    version = latest.strftime("%Y-%m-%d")

    logger.info("Detected TIGA remote version %s", version)
    return version

def DrugCentral_parse_version_from_page(url: str, logger: logging.Logger) -> str:
    """
    Extract DrugCentral version from dump filename:
    e.g. drugcentral.dump.11012023.sql.gz -> 2023-01-11
    """
    logger.info("Fetching DrugCentral version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    links = soup.find_all("a", href=True)

    for a in links:
        href = a["href"]

        # Match: drugcentral.dump.11012023.sql.gz
        match = re.search(
            r"drugcentral\.dump\.(\d{2})(\d{2})(\d{4})\.sql\.gz",
            href,
            flags=re.IGNORECASE
        )

        if not match:
            continue

        day, month, year = match.groups()

        try:
            dt = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date parsed from filename: {href}")

        version = dt.strftime("%Y-%m-%d")
        logger.info("Detected DrugCentral remote version %s", version)
        return version

    raise ValueError(
        "DrugCentral dump file not found on download page "
        "(expected drugcentral.dump.<DDMMYYYY>.sql.gz)"
    )

def FooDB_parse_version_from_page(url: str, logger: logging.Logger) -> str:
    logger.info("Fetching FooDB version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table", class_="table-standard")
    if not tables:
        raise ValueError("FooDB: no tables found on download page.")

    dates = []

    for table in tables:
        thead = table.find("thead")
        if not thead:
            continue

        headers = [th.get_text(strip=True) for th in thead.find_all("th")]

        if "Date Added" not in headers:
            continue

        date_idx = headers.index("Date Added")

        tbody = table.find("tbody")
        if not tbody:
            continue

        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) <= date_idx:
                continue

            raw_date = cells[date_idx].get_text(strip=True)

            try:
                # Example: "April 7 2020", "October 13 2022"
                dt = datetime.strptime(raw_date, "%B %d %Y")
                dates.append(dt)
            except ValueError:
                logger.warning("FooDB: skipping unparsable date '%s'", raw_date)

    if not dates:
        raise ValueError("FooDB: no valid 'Date Added' values found.")

    latest = max(dates)
    version = latest.strftime("%Y-%m-%d")

    logger.info("Detected FooDB remote version %s", version)
    return version

def HPA_parse_version_from_page(url: str, logger: logging.Logger) -> str:
    logger.info("Fetching HPA version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    page_text = soup.get_text(" ", strip=True)

    match = re.search(
        r"Human Protein Atlas\s+version\s+([\d.]+)",
        page_text,
        flags=re.IGNORECASE
    )

    if not match:
        raise ValueError(
            "Could not find 'Human Protein Atlas version X.Y' in page text."
        )

    version_number = match.group(1).rstrip(".")
    formatted_version = f"v{version_number}"

    logger.info("Detected HPA remote version %s", formatted_version)
    return formatted_version

def ChEMBL_parse_version_from_page(
    url: str, 
    filename: str, 
    logger: logging.Logger
) -> str:
    """
    Extract the 'Last modified' date for a specific file from ChEMBL directory listing.
    
    Args:
        url: URL of the ChEMBL directory listing page
        filename: Name or partial name of the file to look for (e.g., 'chembl_36_sqlite')
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format
        
    Raises:
        ValueError: If table structure is unexpected or file not found
    """
    logger.info("Fetching ChEMBL version for file '%s' from %s", filename, url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table (there's only one in directory listings)
    table = soup.find("table")
    if not table:
        raise ValueError("ChEMBL: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("ChEMBL: table has insufficient rows.")
    
    # Find the row containing our filename
    target_row = None
    for row in rows:
        # Look for <a> tag with matching filename
        link = row.find("a")
        if link and filename in link.get_text(strip=True):
            target_row = row
            break
    
    if not target_row:
        raise ValueError(f"ChEMBL: file containing '{filename}' not found in directory listing.")
    
    # Extract all cells from the row
    cells = target_row.find_all("td")
    if len(cells) < 3:
        raise ValueError(f"ChEMBL: row for '{filename}' has insufficient columns.")
    
    # Last modified is in the 3rd column (index 2)
    raw_date = cells[2].get_text(strip=True)
    
    # Parse the date (format: "YYYY-MM-DD HH:MM")
    try:
        dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
        version = dt.strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"ChEMBL: could not parse date '{raw_date}': {e}")
    
    logger.info("Detected ChEMBL remote version %s for file '%s'", version, filename)
    return version

def MarkerDB_parse_version_from_page(url: str, logger: logging.Logger) -> str:
    logger.info("Fetching MarkerDB version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the table containing "Released On"
    table = soup.find("table", class_="table-standard")
    if not table:
        raise ValueError("MarkerDB: could not find data table.")

    thead = table.find("thead")
    if not thead:
        raise ValueError("MarkerDB: table header not found.")

    headers = [th.get_text(strip=True) for th in thead.find_all("th")]

    if "Released On" not in headers:
        raise ValueError("MarkerDB: 'Released On' column not found.")

    released_idx = headers.index("Released On")

    tbody = table.find("tbody")
    if not tbody:
        raise ValueError("MarkerDB: table body not found.")

    dates = []

    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) <= released_idx:
            continue

        raw_date = cells[released_idx].get_text(strip=True)

        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d")
            dates.append(dt)
        except ValueError:
            logger.warning("Skipping unparsable date: %s", raw_date)

    if not dates:
        raise ValueError("MarkerDB: no valid release dates found.")

    latest = max(dates)
    version = latest.strftime("%Y-%m-%d")

    logger.info("Detected MarkerDB remote version %s", version)
    return version