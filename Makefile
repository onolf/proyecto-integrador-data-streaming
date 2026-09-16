.PHONY: install dataset run demo stop logs test check smoke clean-data

install:
	uv sync

dataset:
	uv run python -m apu_streaming.dataset

run:
	docker compose up --build

demo:
	docker compose up --build

stop:
	docker compose down

logs:
	docker compose logs --follow --tail=100

test:
	uv run pytest

check:
	uv run ruff check .

smoke:
	docker compose --profile smoke up --build --abort-on-container-exit --exit-code-from smoke smoke

clean-data:
	uv run python -m apu_streaming.dataset --clean
