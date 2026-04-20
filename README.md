# cu 3rd view

Backend for collecting and serving university schedule data, matching recordings, and running a KTalk attendance worker.

## Structure

```text
app/
  api/routes/        FastAPI route modules by domain
  core/              config and logging
  integrations/      Yandex and KTalk clients
  services/          shared helpers
  scripts/           operational utilities
  workers/           background worker entrypoints
  templates/         HTML pages
tests/               pytest suite
pyproject.toml       uv project metadata
uv.lock              locked dependency graph
docker-compose.yml   local development stack
Dockerfile           container image
```

## Configuration

Copy `example.env` to `.env` and fill in the real values.

Required runtime files:

1. `.env`
2. `cookie.txt`
3. `ktalk_auth.txt`

The app reads configuration only from environment variables and file paths declared in `.env`.

## Local Development

Install dependencies:

```bash
uv sync --dev
```

Run the API:

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8082
```

Run the worker:

```bash
uv run cu-3rd-view-worker
```

Run tests:

```bash
uv run pytest
```

## Docker Compose

Start local development stack:

```bash
cp example.env .env
docker compose up --build
```

Services:

- `web`: FastAPI app with live reload
- `worker`: background KTalk/Yandex worker
- `db`: PostgreSQL 16

## Utility Scripts

Populate database from CSV:

```bash
uv run cu-3rd-view-parse
```

Insert a manual test event:

```bash
uv run cu-3rd-view-add-test-event
```

## CI

GitHub Actions runs `pytest` on every push and pull request via [.github/workflows/tests.yml](.github/workflows/tests.yml).

---
