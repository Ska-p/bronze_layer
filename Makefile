# -----------------------------
# Configurazione
# -----------------------------
ACR_NAME=dompedatafusiontest
IMAGE_NAME=bronze_layer
TAG=latest

# Ottiene automaticamente il loginServer dell'ACR
ACR_FQDN=$(shell az acr show --name $(ACR_NAME) --query "loginServer" -o tsv)

.PHONY: build push

# Build dell'immagine Docker
build:
	docker build --platform=linux/amd64 -t $(IMAGE_NAME):$(TAG) .

# Push dell'immagine su ACR (include tag + login)
push: build
	docker tag $(IMAGE_NAME):$(TAG) $(ACR_FQDN)/$(IMAGE_NAME):$(TAG)
	az acr login --name $(ACR_NAME)
	docker push $(ACR_FQDN)/$(IMAGE_NAME):$(TAG)
