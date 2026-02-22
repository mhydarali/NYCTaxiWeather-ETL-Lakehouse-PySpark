.PHONY: sync test lint run

UV := .uv-bootstrap/bin/uv
UV_CACHE_DIR := .uv-cache

sync:
	@if [ ! -x "$(UV)" ]; then \
		python3 -m venv .uv-bootstrap && .uv-bootstrap/bin/pip install uv; \
	fi
	UV_CACHE_DIR=$(UV_CACHE_DIR) $(UV) sync --extra dev

test:
	.venv/bin/python -m pytest -q

lint:
	.venv/bin/python -m ruff check .

run:
	.venv/bin/python scripts/run_all.py --mode local
