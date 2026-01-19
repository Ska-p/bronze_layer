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



url = "https://markerdb.ca/downloads"
url = "https://bioportal.bioontology.org/ontologies/ICD10CM?p=summary"
url = "https://ftp.ebi.ac.uk/pub/databases/chebi/generic_dumps/generic_dump_allstar/"
url = "https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/"
url = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/"
url = "https://ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/"
url = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/"
url = "https://ftp.ebi.ac.uk/pub/databases/chebi/generic_dumps/generic_dump_allstar/"
url = "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html?_gl=1*1a3rnaa*_ga*MjMwNzM5NzQ5LjE3NjQwMjI0ODU.*_ga_7147EPK006*czE3Njg4MTI1NjYkbzExJGcxJHQxNzY4ODEyODA5JGozNSRsMCRoMA..*_ga_P1FPTH9PL4*czE3Njg4MTI1NjYkbzExJGcxJHQxNzY4ODEyODA5JGozNSRsMCRoMA.."

response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html.parser")
links = [a.get("href") for a in soup.find_all("a")]

print(html)       # raw HTML text
for link in links:
    print(link)