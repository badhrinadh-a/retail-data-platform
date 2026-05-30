# Branching Strategy

Our CI/CD model is built around GitFlow principles modified for a single environment:

- **main**: Represents the production state. Pushes here trigger Docker rebuilds and container deployments via Jenkins.
- **develop**: Integration branch. Code must pass linting and unit tests before merging to main.
- **feature/***: Feature branches. E.g., `feature/add-inventory-pipeline`. PRs will trigger automated testing.
- **bugfix/***: For urgent fixes outside the normal feature release cycle.
