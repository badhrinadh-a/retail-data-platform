# CI/CD Flow

The platform supports two continuous integration and deployment pipelines:
1. **GitHub Actions** (Alternative, modern production-standard CI/CD)
2. **Jenkins** (Legacy / locally-run alternative)

---

## 1. GitHub Actions Pipeline

Our primary CI/CD flow is defined in [.github/workflows/ci-cd.yml](file:///home/bhaskar/projects/retail-data-platform/.github/workflows/ci-cd.yml).

### Workflow Triggers
- **Triggers**: Pull requests or direct pushes targeting `main` and `develop`.

### Pipeline Jobs & Stages

1. **Lint & Format Check (`lint-format` job)**
   - Checks code formatting using `black` and import ordering using `isort` with `--check` flags.
   - Runs `flake8` static code analysis.
   - Runs on all branches to prevent styling regressions.

2. **Unit Tests (`unit-tests` job)**
   - Installs dependencies from `requirements.txt`.
   - Automatically sets up Java 11 (required for PySpark / Spark SQL context) and Python.
   - Executes pytest suite (`pytest tests/unit/`).

3. **Build & Smoke Test (`build-and-smoke-test` job)**
   - Depends on `lint-format` and `unit-tests`.
   - **Docker Build & Push**:
     - Builds the Docker image for Airflow (`docker/Dockerfile.airflow`).
     - On merges/pushes to `main`, logs in and publishes the tagged images to **Docker Hub** (tags: `latest` and git commit SHA).
   - **Local Deployment & Integration Smoke Testing**:
     - Prepares a clean `.env` config in the runner.
     - Boots up the complete Docker Compose stack (`docker compose up -d`).
     - Polls the Airflow and MinIO health check endpoints until healthy.
     - Cleans up and tears down container resources securely.

### Required GitHub Secrets
To enable publishing images to Docker Hub on merge to `main`, configure the following repository secrets in GitHub (`Settings > Secrets and variables > Actions`):
- `DOCKER_USERNAME`: Your Docker Hub username.
- `DOCKER_PASSWORD`: Your Docker Hub password or Personal Access Token (PAT).

*Note: If these secrets are not configured, the pipeline will build the image to verify it builds successfully, but will skip the login/push step.*

---

## 2. Jenkins Pipeline (Legacy/Local)

Defined in [jenkins/Jenkinsfile](file:///home/bhaskar/projects/retail-data-platform/jenkins/Jenkinsfile). Run locally by logging into the Jenkins container at `http://localhost:8082`.

### Pipeline Stages
1. **Checkout**: Pull from version control.
2. **Lint & Format**: Run `make format` and `make lint` on the agent.
3. **Unit Tests**: Run `pytest` to verify python modules.
4. **Build Image**: Rebuilds the Airflow image on `main`.
5. **Deploy Local**: Triggers a `docker-compose up -d` on the host machine.
6. **Smoke Tests**: Verifies core web server and object storage health post-deploy.

