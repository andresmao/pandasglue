.PHONY: test

init:
	pip install -r requirements-dev.txt

test:
	tox

coverage:
	coverage report

coverage-html:
	coverage html

formatting:
	black pandasglue tests

lint:
	flake8

build: formatting test
	rm -fr build dist .egg requests.egg-info
	python setup.py sdist bdist_wheel

publish: build
	twine upload dist/*
	rm -fr build dist .egg requests.egg-info