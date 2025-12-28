import requests
from bs4 import BeautifulSoup

# http
# https://markerdb.ca/downloads"

# https://download.baderlab.org/PathwayCommons/PC2/v14/
# https://www.ebi.ac.uk/gwas/docs/file-downloads

# https://www.wikipathways.org/json/index.html

# https://diseases.jensenlab.org/Downloads

# https://foodb.ca/downloads
# https://www.proteinatlas.org/about/download

# Javascript
# https://www.clinpgx.org/downloads
# https://www.ebi.ac.uk/QuickGO/annotations?taxonId=9606&taxonUsage=descendants
# https://foodb.ca/downloads
# https://platform.opentargets.org/downloads
# https://unmtid-shinyapps.net/shiny/tiga/


url = "https://download.baderlab.org/PathwayCommons/PC2/v14/"

response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html.parser")
links = [a.get("data-prefix") for a in soup.find_all("bucket-list")]

print(html)       # raw HTML text
for link in links:
    print(link)