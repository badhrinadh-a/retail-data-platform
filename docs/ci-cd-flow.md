# CI/CD Flow

The platform uses Jenkins to verify code that should already pass local gates.

## Local gate (first line of defense)

1. `make install-dev` — `.venv`, `requirements.txt`, pre-commit hooks
2. On commit — pre-commit runs black, isort, flake8
3. Before push — `make format && make lint && make test` (or `make check`)

CI is a **final verification**, not a discovery mechanism.

## Pipeline stages

1. **Checkout**: Pull from version control.
2. **Lint & Format**: `make format-check lint` (read-only; dependencies pre-installed in Jenkins image).
3. **Unit Tests**: `make test` (pytest; no workspace venv or apt-get in pipeline).
4. **Build Image**: Only on `main`. Rebuilds the Airflow image.
5. **Deploy Local**: Only on `main`. Restarts docker-compose stack.
6. **Smoke Tests**: Verifies core services are alive post-deployment.

## Jenkins image

`docker/Dockerfile.jenkins` installs:

- `libpq-dev` and `gcc` for native Python extensions
- All packages from `requirements.txt` at **image build** time

Rebuild Jenkins after dependency changes:

```bash
make build
# or: docker-compose build jenkins && docker-compose up -d jenkins
```
