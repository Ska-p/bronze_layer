ifneq ("$(wildcard src/.env)","")
    include src/.env
    export $(shell sed 's/=.*//' .env)
endif

# Map .env names to the variables used in the script
# This assumes your .env uses names like AZ_BATCH_ACCOUNT_NAME
BATCH_ACCOUNT_NAME=$(AZ_BATCH_ACCOUNT_NAME)
BATCH_ACCOUNT_KEY=$(AZ_BATCH_ACCOUNT_KEY)
BATCH_ACCOUNT_URL=$(AZ_BATCH_ACCOUNT_URL)
BATCH_POOL_ID=bronze_pool

ACR_NAME=dompedatafusiontest
IMAGE_NAME=bronze_layer
TAG=latest

ACR_FQDN=$(shell az acr show --name $(ACR_NAME) --query "loginServer" -o tsv)

.PHONY: build push refresh-pool

build:
	docker build --platform=linux/amd64 -t $(IMAGE_NAME):$(TAG) .

# refresh-pool:
# 	@echo "Fetching nodes from pool: $(BATCH_POOL_ID)..."
# 	@nodes=$$(az batch node list \
# 		--pool-id $(BATCH_POOL_ID) \
# 		--account-name $(BATCH_ACCOUNT_NAME) \
# 		--account-key $(BATCH_ACCOUNT_KEY) \
# 		--account-endpoint $(BATCH_ACCOUNT_URL) \
# 		--query "[].id" -o tsv); \
# 	if [ -z "$$nodes" ]; then \
# 		echo "No nodes found in pool $(BATCH_POOL_ID)."; \
# 	else \
# 		for node in $$nodes; do \
# 			echo "Rebooting node: $$node"; \
# 			az batch node reboot \
# 				--pool-id $(BATCH_POOL_ID) \
# 				--node-id $$node \
# 				--account-name $(BATCH_ACCOUNT_NAME) \
# 				--account-key $(BATCH_ACCOUNT_KEY) \
# 				--account-endpoint $(BATCH_ACCOUNT_URL) \
# 				--node-reboot-option requeue; \
# 		done; \
# 	fi

push: build
	docker tag $(IMAGE_NAME):$(TAG) $(ACR_FQDN)/$(IMAGE_NAME):$(TAG)
	az acr login --name $(ACR_NAME)
	docker push $(ACR_FQDN)/$(IMAGE_NAME):$(TAG)
# 	$(MAKE) refresh-pool