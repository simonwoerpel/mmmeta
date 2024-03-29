all: clean install test

install:
	pip install -e .
	pip install twine coverage nose moto pytest pytest-cov black flake8 isort bump2version

test:
	pytest -s --cov=mmmeta --cov-report term-missing

build:
	python setup.py sdist bdist_wheel

prerelease: test readme
	bump2version patch

release: clean build
	twine upload dist/*

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

readme:
	pandoc README.md -o README.rst
