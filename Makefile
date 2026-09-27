.PHONY: install check test integration up down load
PYTHON ?= .venv/bin/python
install:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements.lock
	$(PYTHON) -m pip install --no-deps -e .
check:
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m ruff check .
test:
	$(PYTHON) -m pytest -q
integration:
	RUN_INTEGRATION=1 $(PYTHON) -m pytest -q
up:
	docker compose up --build --wait --wait-timeout 180
down:
	docker compose down
load:
	$(PYTHON) -m scripts.load_test --events 2000 --concurrency 16 --message-bytes 512 --output artifacts/load.json
