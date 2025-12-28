import requests
from pathlib import Path

BASE_URL = "https://www.ebi.ac.uk/QuickGO/services/annotation/downloadSearch"
OUTPUT_FILE = Path("quickgo_annotations.gpad")

params = {
    "taxonId": "9606",
    "taxonUsage": "descendants"
}

headers = {
    "Accept": "text/gpad"   # or text/gaf
}

response = requests.get(
    BASE_URL,
    params=params,          # ✅ QUERY PARAMS
    headers=headers,
    stream=True,
    timeout=300
)

response.raise_for_status()

with OUTPUT_FILE.open("wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)

print(f"Download completed: {OUTPUT_FILE.resolve()}")

import requests
import sys
import json

URL = "https://www.ebi.ac.uk/QuickGO/services/annotation/about"

headers = {
    "Accept": "application/json"
}

r = requests.get(URL, headers=headers, timeout=60)

if not r.ok:
    r.raise_for_status()
    sys.exit(1)

data = r.json()

# Pretty print full response
print(json.dumps(data, indent=2))
