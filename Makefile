.PHONY: test clean install-dev-local format

install-dev-local:
	pip install --upgrade pip
	pre-commit install

clean:
	rm -rf spark-warehouse
	rm -rf .bsp
	sbt clean

format:
	sbt scalafmt

test:
	sbt -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED test
	@make clean
