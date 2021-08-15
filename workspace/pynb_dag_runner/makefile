SHELL := /bin/bash

test-pytest:
	echo ">> Checking unit tests (RUN_ENVIRONMENT = ${RUN_ENVIRONMENT})"
	pytest --durations=10 --continue-on-collection-errors tests

test-mypy:
	echo ">> Checking mypy type hints ..."
	mypy --ignore-missing-imports --exclude build .

test-black:
	echo ">> Checking black code formatting ..."
	black --check --diff .

build:
	echo ">> Building library wheel file ..."
	make clean

	python3 setup.py bdist_wheel

clean:
	rm -rf build dist pynb_dag_runner.egg-info
