PYTHON_BIN ?= python

format: isort black

black:
	'$(PYTHON_BIN)' -m black src

isort:
	'$(PYTHON_BIN)' -m isort -rc src

venv: requirements.txt
	'$(PYTHON_BIN)' -m pip install -r requirements.txt

requirements.txt: FORCE
	'$(PYTHON_BIN)' -m piptools compile requirements.in

FORCE:
