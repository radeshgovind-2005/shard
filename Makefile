.PHONY: help setup gh-setup build test lint format check run clean
.DEFAULT_GOAL := help

GRADLE := ./gradlew

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

setup: ## Install Node tooling and Git hooks (Husky)
	npm install

gh-setup: ## One-time GitHub config: squash-only merges + main ruleset (needs CI on main)
	./scripts/gh-setup.sh

build: ## Compile without running tests
	$(GRADLE) build -x test

test: ## Run tests
	$(GRADLE) test

lint: ## Check formatting (Spotless + ktlint)
	$(GRADLE) spotlessCheck

format: ## Apply formatting
	$(GRADLE) spotlessApply

check: ## Lint, build and test (what CI and pre-push run)
	$(GRADLE) spotlessCheck build

run: ## Run the application
	$(GRADLE) run

clean: ## Remove build outputs
	$(GRADLE) clean
