FUNCTION_NAME?=create_resource
REGION?=us-east1

include ../common.mk

.PHONY: test_httpie
test_httpie: ## Sends a sample request to a function
	$(HTTPIE) -v $(FUNCTION_URL) name="Gretta Renner" email=greta@example.com age=33

