# Local Setup Guide

## Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Make
- On Linux (for `pip install` / `make install-dev`): `libpq-dev`, `librdkafka-dev`, and `gcc`  
  (`sudo apt-get install -y libpq-dev librdkafka-dev gcc` on Debian/Ubuntu)

## Python development environment

Install dependencies and pre-commit hooks once per clone:

```bash
make install-dev
source .venv/bin/activate
```

This creates `.venv` (gitignored), installs `requirements.txt` and dev tools, and registers git pre-commit hooks for **black**, **isort**, and **flake8**.

### Before every commit

Pre-commit runs automatically on `git commit`. You can also verify manually:

```bash
make format && make lint && make test
```

Or run the read-only CI-equivalent gate (no file changes):

```bash
make check
```

`make check` runs `format-check`, `lint`, and `test` — the same checks Jenkins runs after checkout.

## Infrastructure

1. **Start the stack**
   ```bash
   make build
   make up
   ```
   This starts Kafka, Spark, MinIO, Postgres, Airflow, and Jenkins.

2. **Wait for services**
   Wait approximately 2–3 minutes for Airflow to initialize its database and Kafka to start.

3. **Bootstrap environment**
   ```bash
   ./scripts/setup_minio.sh
   ./scripts/bootstrap.sh
   ```

4. **Accessing UIs**
   - **Airflow**: http://localhost:8085 (admin/admin)
   - **Jenkins**: http://localhost:8082
   - **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
   - **Spark UI**: http://localhost:8080

5. **Stopping the stack**
   ```bash
   make down
   ```

## Jenkins CI image

After changing `requirements.txt` or `docker/Dockerfile.jenkins`, rebuild the Jenkins image so CI uses baked dependencies:

```bash
docker-compose build jenkins
docker-compose up -d jenkins
```
