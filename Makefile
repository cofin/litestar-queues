SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

# =============================================================================
# Configuration and Environment Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
UV_SYNC_ARGS ?= --all-extras --dev

# -----------------------------------------------------------------------------
# Display Formatting and Colors
# -----------------------------------------------------------------------------
BLUE := $(shell printf "\033[1;34m")
GREEN := $(shell printf "\033[1;32m")
RED := $(shell printf "\033[1;31m")
YELLOW := $(shell printf "\033[1;33m")
NC := $(shell printf "\033[0m")
INFO := $(shell printf "$(BLUE)i$(NC)")
OK := $(shell printf "$(GREEN)ok$(NC)")
WARN := $(shell printf "$(YELLOW)!$(NC)")
ERROR := $(shell printf "$(RED)x$(NC)")

# =============================================================================
# Help and Documentation
# =============================================================================

.PHONY: help
help:                                               ## Display this help text for Makefile
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# =============================================================================
# Installation and Environment Setup
# =============================================================================

.PHONY: install-uv
install-uv:                                         ## Install latest version of uv
	@echo "${INFO} Installing uv..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
	@echo "${OK} UV installed successfully"

.PHONY: install
install: clean                                      ## Install the project and dependencies for local development
	@echo "${INFO} Starting fresh installation..."
	@uv sync $(UV_SYNC_ARGS)
	@echo "${OK} Installation complete"

.PHONY: destroy
destroy:                                            ## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment..."
	@rm -rf .venv
	@echo "${OK} Virtual environment destroyed"

# =============================================================================
# Dependency Management
# =============================================================================

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies to latest stable versions
	@echo "${INFO} Updating all dependencies..."
	@uv lock --upgrade
	@echo "${OK} Dependencies updated"
	@uvx prek autoupdate
	@echo "${OK} Updated prek hooks"

.PHONY: lock
lock:                                               ## Rebuild lockfiles from scratch
	@echo "${INFO} Rebuilding lockfiles..."
	@uv lock --upgrade
	@echo "${OK} Lockfiles updated"

# =============================================================================
# Build and Release
# =============================================================================

.PHONY: build
build:                                              ## Build the project
	@echo "${INFO} Building package..."
	@uv build
	@echo "${OK} Package build complete"

# =============================================================================
# Documentation
# =============================================================================

.PHONY: docs
docs:                                               ## Build documentation
	@echo "${INFO} Building docs..."
	@uv run sphinx-build -b html docs docs/_build/html
	@echo "${OK} Docs build complete"

.PHONY: docs-linkcheck
docs-linkcheck:                                     ## Check documentation links
	@echo "${INFO} Checking docs links..."
	@uv run sphinx-build -b linkcheck docs docs/_build/linkcheck
	@echo "${OK} Docs linkcheck complete"

.PHONY: docs-clean
docs-clean:                                         ## Clean documentation artifacts
	@echo "${INFO} Cleaning docs artifacts..."
	@rm -rf docs/_build docs-build >/dev/null 2>&1
	@echo "${OK} Docs artifacts cleaned"

# =============================================================================
# Cleaning and Maintenance
# =============================================================================

.PHONY: clean
clean:                                              ## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory..."
	@rm -rf .pytest_cache .ruff_cache .hypothesis build/ dist/ .eggs/ .coverage coverage.xml coverage.json htmlcov/ src/tests/.pytest_cache src/tests/**/.pytest_cache .mypy_cache >/dev/null 2>&1
	@find . -name '*.egg-info' -exec rm -rf {} + >/dev/null 2>&1
	@find . -type f -name '*.egg' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyc' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyo' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*~' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '__pycache__' -exec rm -rf {} + >/dev/null 2>&1
	@find . -name '.ipynb_checkpoints' -exec rm -rf {} + >/dev/null 2>&1
	@echo "${OK} Working directory cleaned"

# =============================================================================
# Testing and Quality Checks
# =============================================================================

.PHONY: test
test:                                               ## Run the tests
	@echo "${INFO} Running test cases..."
	@uv run pytest src/tests
	@echo "${OK} Tests complete"

.PHONY: test-all
test-all: test                                      ## Run all tests

.PHONY: test-unit
test-unit:                                          ## Run unit tests only (no Docker required)
	@echo "${INFO} Running unit tests..."
	@uv run pytest src/tests/unit -n auto
	@echo "${OK} Unit tests complete"

.PHONY: test-integration
test-integration:                                   ## Run integration tests only (autoskips without Docker)
	@echo "${INFO} Running integration tests..."
	@uv run pytest src/tests/integration -n auto
	@echo "${OK} Integration tests complete"

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@echo "${INFO} Running tests with coverage..."
	@uv run pytest src/tests --cov -n auto
	@uv run coverage html >/dev/null 2>&1
	@uv run coverage xml >/dev/null 2>&1
	@echo "${OK} Coverage report generated"

# -----------------------------------------------------------------------------
# Type Checking
# -----------------------------------------------------------------------------

.PHONY: mypy
mypy:                                               ## Run mypy
	@echo "${INFO} Running mypy..."
	@uv run dmypy run
	@echo "${OK} Mypy checks passed"

.PHONY: mypy-nocache
mypy-nocache:                                       ## Run Mypy without cache
	@echo "${INFO} Running mypy without cache..."
	@uv run mypy
	@echo "${OK} Mypy checks passed"

.PHONY: pyright
pyright:                                            ## Run pyright
	@echo "${INFO} Running pyright..."
	@uv run pyright
	@echo "${OK} Pyright checks passed"

.PHONY: type-check
type-check: mypy pyright                            ## Run all type checking

# -----------------------------------------------------------------------------
# Linting and Formatting
# -----------------------------------------------------------------------------

.PHONY: prek
prek:                                               ## Run prek hooks
	@echo "${INFO} Running prek checks..."
	@uvx prek run --show-diff-on-failure --color=always --all-files
	@echo "${OK} prek checks passed"

.PHONY: slotscheck
slotscheck:                                         ## Run slotscheck
	@echo "${INFO} Running slots check..."
	@uv run slotscheck src/litestar_queues/
	@echo "${OK} Slots check passed"

.PHONY: fix
fix:                                                ## Fix linting issues
	@echo "${INFO} Fixing linting issues..."
	@uv run ruff check --fix --unsafe-fixes src/
	@uv run ruff format src/
	@echo "${OK} Linting issues fixed"

.PHONY: lint
lint: prek type-check slotscheck                    ## Run all linting checks

.PHONY: check-all
check-all: lint test-all coverage                   ## Run all checks (lint, test, coverage)
