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

def TIGA_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract version from Apache directory listing by finding the 'Last modified' 
    timestamp of the specified file.
    """
    logger.info("Fetching TIGA version for file '%s' from %s", filename, url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    rows = soup.find_all("tr")
    if not rows:
        raise ValueError("TIGA: no rows found in directory listing.")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        link = cols[1].find("a")
        if not link:
            continue

        file_name = link.get_text(strip=True)

        # Match the specific filename we're looking for
        if filename not in file_name:
            continue

        raw_ts = cols[2].get_text(strip=True)
        if not raw_ts:
            continue

        try:
            # Format: YYYY-MM-DD HH:MM
            dt = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M")
            version = dt.strftime("%Y-%m-%d")
            logger.info("Detected TIGA remote version %s for file '%s'", version, file_name)
            return version
        except ValueError:
            logger.warning("TIGA: could not parse timestamp '%s'", raw_ts)
            continue

    raise ValueError(f"TIGA: file containing '{filename}' not found in directory listing.")

def DrugCentral_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract DrugCentral version from dump filename:
    e.g. drugcentral.dump.11012023.sql.gz -> 2023-01-11
    """
    logger.info("Fetching DrugCentral version from %s", url)
    import pdb
    pdb.set_trace()
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

def FooDB_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    logger.info("Fetching FooDB version from %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table", class_="table-standard")
    if not tables:
        raise ValueError("FooDB: no tables found on download page.")

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

            # Check if this row contains both filename AND 'mysql'
            row_text = " ".join(cell.get_text(strip=True) for cell in cells).lower()
            if filename.lower() not in row_text or "mysql" not in row_text:
                continue

            raw_date = cells[date_idx].get_text(strip=True)

            try:
                # Example: "April 7 2020", "October 13 2022"
                dt = datetime.strptime(raw_date, "%B %d %Y")
                version = dt.strftime("%Y-%m-%d")
                logger.info("Detected FooDB remote version %s", version)
                return version
            except ValueError:
                logger.warning("FooDB: skipping unparsable date '%s'", raw_date)
                continue

    raise ValueError("FooDB: matching file not found in download table.")

def HPA_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
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

def MarkerDB_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
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

def GWASCATALOG_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract the 'Last modified' date for a specific file from GWAS Catalog directory listing.
    
    Args:
        url: URL of the GWAS Catalog directory listing page
        filename: Name or partial name of the file to look for (e.g., 'ontology-annotated-full')
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format
        
    Raises:
        ValueError: If table structure is unexpected or file not found
    """
    logger.info("Fetching GWAS Catalog version from %s", url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table
    table = soup.find("table")
    if not table:
        raise ValueError("GWAS Catalog: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("GWAS Catalog: table has insufficient rows.")
    
    # Find the row containing our filename
    for row in rows:
        # Look for <a> tag with matching filename
        link = row.find("a")
        if not link or filename not in link.get_text(strip=True):
            continue
        
        # Extract all cells from the row
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        
        # Last modified is in the 3rd column (index 2)
        raw_date = cells[2].get_text(strip=True)
        
        # Parse the date (format: "YYYY-MM-DD HH:MM")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
            version = dt.strftime("%Y-%m-%d")
            logger.info("Detected GWAS Catalog remote version %s", version)
            return version
        except ValueError as e:
            raise ValueError(f"GWAS Catalog: could not parse date '{raw_date}': {e}")
    
    raise ValueError("GWAS Catalog: matching file not found in directory listing.")

def ClinVar_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract the 'Last Modified' date for a specific file from ClinVar directory listing.
    
    Args:
        url: URL of the ClinVar directory listing page
        filename: Name or partial name of the file to look for (e.g., 'variant_summary')
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format
        
    Raises:
        ValueError: If table structure is unexpected or file not found
    """
    logger.info("Fetching ClinVar version from %s", url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table
    table = soup.find("table")
    if not table:
        raise ValueError("ClinVar: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("ClinVar: table has insufficient rows.")
    
    # Find the row containing our filename
    for row in rows:
        # Look for <a> tag with matching filename
        link = row.find("a")
        if not link or filename not in link.get_text(strip=True):
            continue
        
        # Extract all cells from the row
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        
        # Last Modified is in the 4th column (index 3)
        raw_date = cells[3].get_text(strip=True)
        if not raw_date:
            continue
        
        # Parse the date (format: "YYYY-MM-DD HH:MM:SS")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S")
            version = dt.strftime("%Y-%m-%d")
            logger.info("Detected ClinVar remote version %s", version)
            return version
        except ValueError as e:
            raise ValueError(f"ClinVar: could not parse date '{raw_date}': {e}")
    
    raise ValueError("ClinVar: matching file not found in directory listing.")

def UniProt_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract the 'Last modified' date for UniProt .dat.gz files from directory listing.
    Finds all .dat.gz files containing 'trembl' or 'sprot' and returns the latest timestamp.
    
    Args:
        url: URL of the UniProt directory listing page
        filename: Not used directly (checks for both trembl and sprot)
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format (latest among matching files)
        
    Raises:
        ValueError: If table structure is unexpected or no matching files found
    """
    logger.info("Fetching UniProt version from %s", url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table
    table = soup.find("table")
    if not table:
        raise ValueError("UniProt: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("UniProt: table has insufficient rows.")
    
    timestamps = []
    
    # Find all rows containing .dat.gz files with trembl or sprot
    for row in rows:
        link = row.find("a")
        if not link:
            continue
        
        file_name = link.get_text(strip=True).lower()
        
        # Check if file is .dat.gz and contains trembl or sprot
        if not file_name.endswith(".dat.gz"):
            continue
        
        if "trembl" not in file_name and "sprot" not in file_name:
            continue
        
        # Extract all cells from the row
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        
        # Last modified is in the 3rd column (index 2)
        raw_date = cells[2].get_text(strip=True)
        if not raw_date:
            continue
        
        # Parse the date (format: "YYYY-MM-DD HH:MM")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
            timestamps.append(dt)
        except ValueError:
            logger.warning("UniProt: could not parse timestamp '%s'", raw_date)
            continue
    
    if not timestamps:
        raise ValueError("UniProt: no .dat.gz files with 'trembl' or 'sprot' found in directory listing.")
    
    # Return the latest timestamp among matching files
    latest = max(timestamps)
    version = latest.strftime("%Y-%m-%d")
    logger.info("Detected UniProt remote version %s", version)
    return version

def OpenTargets_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract the 'Last modified' date of the 'output/' folder from OpenTargets directory listing.
    
    Args:
        url: URL of the OpenTargets /latest/ directory listing page
        filename: Not used (kept for signature consistency)
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format
        
    Raises:
        ValueError: If table structure is unexpected or output folder not found
    """
    
    url = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/"
    
    logger.info("Fetching OpenTargets version from %s", url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table
    table = soup.find("table")
    if not table:
        raise ValueError("OpenTargets: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("OpenTargets: table has insufficient rows.")
    
    # Find the row containing the output/ folder
    for row in rows:
        link = row.find("a")
        if not link:
            continue
        
        folder_name = link.get_text(strip=True)
        if folder_name != "output/":
            continue
        
        # Extract all cells from the row
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        
        # Last modified is in the 3rd column (index 2)
        raw_date = cells[2].get_text(strip=True)
        if not raw_date:
            continue
        
        # Parse the date (format: "YYYY-MM-DD HH:MM")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
            version = dt.strftime("%Y-%m-%d")
            logger.info("Detected OpenTargets remote version %s", version)
            return version
        except ValueError as e:
            raise ValueError(f"OpenTargets: could not parse date '{raw_date}': {e}")
    
    raise ValueError("OpenTargets: 'output/' folder not found in directory listing.")

def ChEBI_SQL_parse_version_from_page(url: str, filename: str, logger: logging.Logger) -> str:
    """
    Extract the 'Last modified' date from the first .sql.zip file in ChEBI directory listing.
    
    Args:
        url: URL of the ChEBI directory listing page
        filename: Not used (kept for signature consistency)
        logger: Logger instance for logging
        
    Returns:
        Version string in YYYY-MM-DD format
        
    Raises:
        ValueError: If table structure is unexpected or no .sql.zip files found
    """
    logger.info("Fetching ChEBI SQL version from %s", url)
    
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find the main table
    table = soup.find("table")
    if not table:
        raise ValueError("ChEBI SQL: could not find directory listing table.")
    
    # Find all table rows
    rows = table.find_all("tr")
    if len(rows) < 2:  # Need at least header + one data row
        raise ValueError("ChEBI SQL: table has insufficient rows.")
    
    # Find the first .sql.zip file
    for row in rows:
        link = row.find("a")
        if not link:
            continue
        
        file_name = link.get_text(strip=True)
        if not file_name.endswith(".sql.zip"):
            continue
        
        # Extract all cells from the row
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        
        # Last modified is in the 3rd column (index 2)
        raw_date = cells[2].get_text(strip=True)
        if not raw_date:
            continue
        
        # Parse the date (format: "YYYY-MM-DD HH:MM")
        try:
            dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
            version = dt.strftime("%Y-%m-%d")
            logger.info("Detected ChEBI SQL remote version %s", version)
            return version
        except ValueError as e:
            raise ValueError(f"ChEBI SQL: could not parse date '{raw_date}': {e}")
    
    raise ValueError("ChEBI SQL: no .sql.zip files found in directory listing.")