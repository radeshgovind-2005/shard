.PHONY: help setup gh-setup build jar test lint format check run clean data data-sample data-size data-clean
.DEFAULT_GOAL := help

GRADLE := ./gradlew

# Dataset selection for the data targets: years (2025), ranges (2011-2026) or months (2025-01), space-separated.
YEARS ?= 2025
SAMPLE ?= 2025-01

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install Node tooling and Git hooks (Husky)
	npm install

gh-setup: ## One-time GitHub config: squash-only merges + main ruleset (needs CI on main)
	./scripts/gh-setup.sh

build: ## Compile without running tests
	$(GRADLE) build -x test

jar: ## Build the fat jar for spark-submit (build/libs/shard-all.jar)
	$(GRADLE) shadowJar

test: ## Run tests
	$(GRADLE) test

lint: ## Check formatting (Spotless + ktlint)
	$(GRADLE) spotlessCheck

format: ## Apply formatting
	$(GRADLE) spotlessApply

check: ## Lint, build and test (what CI and pre-push run)
	$(GRADLE) spotlessCheck build

run: ## Run locally with Spark on the classpath: make run ARGS="..."
	$(GRADLE) run $(if $(ARGS),--args="$(ARGS)")

clean: ## Remove build outputs
	$(GRADLE) clean

data: ## Download trips + zone lookup into data/: make data [YEARS="2024 2025" | YEARS=2011-2026]
	./scripts/download-data.sh $(YEARS)

data-sample: ## Download one month for smoke tests: make data-sample [SAMPLE=2025-01]
	./scripts/download-data.sh $(SAMPLE)

data-size: ## Show download size vs free disk, without downloading: make data-size [YEARS=...]
	./scripts/download-data.sh --dry-run $(YEARS)

data-clean: ## Delete downloaded data to free disk (inputs never change): make data-clean YEARS=2009-2010 | YEARS=all
	@if [ "$(origin YEARS)" != "command line" ]; then \
		echo "data-clean: pass YEARS explicitly, e.g. make data-clean YEARS=2009-2010 or YEARS=all" >&2; exit 2; fi
	./scripts/download-data.sh --delete $(YEARS)
