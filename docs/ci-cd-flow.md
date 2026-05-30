# CI/CD Flow

The platform uses Jenkins to automate testing and local deployment. 

## Pipeline Stages
1. **Checkout**: Pull from version control.
2. **Lint & Format**: Run `black` and `flake8` to enforce style.
3. **Unit Tests**: Run `pytest` to ensure Spark utilities function properly.
4. **Build Image**: Only on `main`. Rebuilds Docker images (Airflow/Jenkins).
5. **Deploy Local**: Only on `main`. Restarts docker-compose stack.
6. **Smoke Tests**: Verifies core services are alive post-deployment.
