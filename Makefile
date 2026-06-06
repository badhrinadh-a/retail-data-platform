.PHONY: help up down restart logs build install-dev ci-deps format format-check lint test check ci clean

VENV := .venv
VENV_PYTHON := $(VENV)/bin/python
VENV_PIP := $(VENV)/bin/pip

ifneq ($(wildcard $(VENV_PYTHON)),)
  PYTHON := $(VENV_PYTHON)
else
  PYTHON := python3
endif

SRC_DIRS := src/ tests/

help:
	@echo "Available commands:"
	@echo "  up            : Start all services in the background"
	@echo "  down          : Stop and remove all services"
	@echo "  restart       : Restart all services"
	@echo "  logs          : Follow logs for all services"
	@echo "  build         : Build custom Docker images"
	@echo "  install-dev   : Create .venv, install deps, enable pre-commit hooks"
	@echo "  ci-deps       : Install requirements-ci.txt (Jenkins lint/test)"
	@echo "  format        : Run black and isort (modifies files)"
	@echo "  format-check  : Verify black/isort without modifying files"
	@echo "  lint          : Run flake8"
	@echo "  test          : Run pytest"
	@echo "  check         : format-check + lint + test (run before push)"
	@echo "  ci            : Same as check (alias for CI parity)"
	@echo "  clean         : Remove python cache files"
	@echo ""
	@echo "Before commit: make format && make lint && make test"
	@echo "Or use pre-commit (after install-dev): hooks run automatically on git commit"

up:
	docker-compose up -d

down:
	docker-compose down -v

restart:
	docker-compose down && docker-compose up -d

logs:
	docker-compose logs -f

build:
	docker-compose build

install-dev:
	python3 -m venv $(VENV)
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -r requirements-dev.txt
	$(VENV)/bin/pre-commit install
	@echo "Dev environment ready. Activate with: source $(VENV)/bin/activate"

ci-deps:
	python3 -m pip install --break-system-packages -r requirements-ci.txt

format:
	$(PYTHON) -m black $(SRC_DIRS)
	$(PYTHON) -m isort $(SRC_DIRS)

format-check:
	$(PYTHON) -m black --check $(SRC_DIRS)
	$(PYTHON) -m isort --check-only $(SRC_DIRS)

lint:
	$(PYTHON) -m flake8 $(SRC_DIRS)

test:
	$(PYTHON) -m pytest tests/

check: format-check lint test

ci: check

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
