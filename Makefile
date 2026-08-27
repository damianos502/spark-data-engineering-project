build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f spark-lab

shell:
	docker compose run --rm spark-lab bash

test:
	docker compose run --rm spark-lab pytest

spark-version:
	docker compose run --rm spark-lab spark-submit --version

python-version:
	docker compose run --rm spark-lab python3 --version
