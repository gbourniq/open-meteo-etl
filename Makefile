PYTHON := python3.13


# SETUP

setup:
	$(PYTHON) -m venv .venv
	source .venv/bin/activate && \
	$(PYTHON) -m pip install poetry==2.0.1 && \
	$(PYTHON) -m poetry install --with dev


# DEVELOPMENT

fmt:
	$(PYTHON) -m autoflake .
	$(PYTHON) -m isort .
	$(PYTHON) -m black .
	$(PYTHON) -m ruff check .

lint:
	$(PYTHON) -m autoflake --check .
	$(PYTHON) -m isort --check-only .
	$(PYTHON) -m black --check .
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy

clean:
	find . -type f \( -name "*.pyc" -o -name ".DS_Store" -o -name "coverage.xml" \) -delete
	find . -type d -name "__pycache__" -execdir mv {} __pycache_renamed \;
	rm -rf open_meteo_etl/.cache
	rm -rf .pytest_cache htmlcov .coverage .mypy_cache .ruff_cache


# UNIT TESTING

test:
	$(PYTHON) -m pytest --cov=open_meteo_etl --cov-report html --cov-report xml

test-coverage: test
	cd htmlcov && $(PYTHON) -m http.server
