FUNCTION_NAME?=process
BUCKET_NAME:=events-stripe-big3
DEPLOY_PARAMS:=--set-env-vars BUCKET_NAME=$(BUCKET_NAME)

include ../common.mk

.PHONY: create_bucket
create_bucket: ## Creates the bucket needed for samples
	gsutil mb -c regional -l $(REGION) gs://$(BUCKET_NAME)

list_bucket: ## List bucket contents
	gsutil ls gs://$(BUCKET_NAME)

.PHONY: test_httpie
test_httpie: ## Sends a sample request to a function
	$(HTTPIE) -v $(FUNCTION_URL) pokemonId=charmander

.PHONY: check_bucket
check_bucket:  ## Retrieves results from bucket
	gsutil cat gs://$(BUCKET_NAME)/pokemons/pokemon-charmander.json
