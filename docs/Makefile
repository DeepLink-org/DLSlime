.PHONY: help serve serve-dirty serve-versioned build clean install check-links deploy-dev deploy-version set-default

PYTHON ?= python
MKDOCS ?= $(shell command -v uv >/dev/null 2>&1 && echo uv run mkdocs || echo mkdocs)
MIKE ?= $(shell command -v uv >/dev/null 2>&1 && echo uv run mike || echo mike)
LINKCHECK ?= $(shell command -v uv >/dev/null 2>&1 && echo uv run mkdocs-linkcheck || echo mkdocs-linkcheck)

help:
	@echo "Available commands:"
	@echo "  make serve        - Serve documentation locally"
	@echo "  make serve-dirty  - Serve with --dirtyreload"
	@echo "  make serve-versioned - Serve versioned docs from the local gh-pages branch"
	@echo "  make build        - Build the static documentation site"
	@echo "  make check-links  - Check documentation links"
	@echo "  make deploy-dev   - Build versioned docs locally into gh-pages branch as dev/latest"
	@echo "  make deploy-version VERSION=x.y.z - Build a release docs version"
	@echo "  make clean        - Remove generated documentation artifacts"

serve:
	$(MKDOCS) serve

serve-dirty:
	$(MKDOCS) serve --dirtyreload

serve-versioned:
	$(MIKE) serve --config-file mkdocs.local.yml --dev-addr 0.0.0.0:18080

build:
	$(MKDOCS) build

install:
	python -m pip install -r requirements.txt

check-links:
	$(LINKCHECK) src

deploy-dev:
	$(MIKE) deploy --config-file mkdocs.local.yml --ignore-remote-status --update-aliases dev latest
	$(MIKE) set-default --ignore-remote-status latest

deploy-version:
	test -n "$(VERSION)"
	$(MIKE) deploy --update-aliases "$(VERSION)" latest
	$(MIKE) set-default latest

set-default:
	test -n "$(VERSION)"
	$(MIKE) set-default "$(VERSION)"

clean:
	rm -rf site/
	rm -rf .cache/
	find . -type d -name "__pycache__" -exec rm -rf {} +
