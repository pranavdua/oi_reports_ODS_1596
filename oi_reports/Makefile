clean:
	poetry env remove python3.8

format:
	poetry run python -m isort **/*.py
	poetry run python -m black **/*.py
	poetry run python -m pyflakes **/*.py

install:
	pip install -i https://pypi.dev.twilio.com/simple poetry==1.1.13 && poetry config --local virtualenvs.in-project true
	poetry install -n --no-dev

test: install
	export PYTHONPATH=.
	poetry run coverage run --source src -m unittest discover  tests 'test_*.py' 
	poetry run coverage xml -i -o coverage.xml 

