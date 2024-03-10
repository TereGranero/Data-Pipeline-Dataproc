FUNCTION_NAME?=create_user
REGION?=us-east1

include ../common.mk

.PHONY: test_httpie
test_httpie: ## Sends a sample request to a function
	$(HTTPIE) -v $(FUNCTION_URL) name="Gretta Renner" email=

