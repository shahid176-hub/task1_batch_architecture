# Task 1 — Batch-processing Data Architecture (skeleton)

This repository shows the **Batch-processing-based data architecture** required by the course assignment.
It contains microservice placeholders, Dockerfiles, `docker-compose.yml`, CI workflow and a step-by-step guide for **GitHub version control** and **Docker updates / runs**.

---

## Quick start (local)
1. Build & start services:
   ```bash
   docker compose build
   docker compose up -d
   ```
2. Check logs:
   ```bash
   docker compose logs -f ingestion-service
   ```
3. Stop services:
   ```bash
   docker compose down
   ```

---

## directory structure 
- ingestion-service/        # ingestion microservice (python/fastapi template)
- batch-processing/         # spark job / scripts (example)
- minio/                    # MinIO config / helpful scripts
- delivery-api/             # API to serve aggregated results (fastapi template)
- analytics/                # notebooks / analysis
- airflow/                  # DAG templates for scheduling
- infra/                    # docker-compose, env files, docs
- .github/workflows/        # CI workflow example

---

## Git / GitHub: step-by-step version control guide
### 1) Initialise repository locally
```bash
git init
git checkout -b main
git add .
git commit -m "chore: initial project skeleton for batch-processing architecture"
```
### 2) Create remote and push (GitHub)
```bash
# create repo on GitHub web UI, then:
git remote add origin git@github.com:<your-org-or-user>/<repo-name>.git
git push -u origin main
```
### 3) Branching model (feature-driven)
- `main` : production-ready, passing CI
- `develop` : integration branch (optional)
- `feature/<short-desc>` : new features
- `hotfix/<id>` : urgent fixes for main

Example workflow:
```bash
git checkout -b feature/ingestion-minio-connector
# code, tests, docs...
git add . && git commit -m "feat: add MinIO connector to ingestion service"
git push -u origin feature/ingestion-minio-connector
# open Pull Request on GitHub; review and merge to develop/main
```

### 4) Commit message style (conventional commits recommended)
Examples:
- `feat: add spark job to aggregate daily metrics`
- `fix: correct timestamp parsing in ingestion`
- `chore: bump python base image to 3.11`
- `docs: add readme for CI workflow`

### 5) Releases & tagging
```bash
git tag -a v0.1.0 -m "Initial working prototype"
git push origin v0.1.0
```
Use semantic versioning: `MAJOR.MINOR.PATCH`.

### 6) GitHub Actions (CI) — included sample in `.github/workflows/ci.yml`:
- Builds Docker images
- Runs basic unit/test (placeholder)
- Lints (optional)
- Example uses `on: push` and PR checks (see file)

---

## Docker image versioning and update process (recommended)
1. Use image tags that match git tags or commit SHA, e.g. `ingestion-service:0.1.0` or `ingestion-service:sha-<short>`.
2. Build locally during development:
   ```bash
   docker compose build ingestion-service
   docker compose up -d ingestion-service
   ```
3. Update code & bump version in `service/VERSION` or in `docker-compose.yml` (tag).
4. Commit and create PR; after merging, push tags:
   ```bash
   git tag -a v0.1.1 -m "Fix timestamp parsing"
   git push origin --tags
   ```
5. CI can pick up tags and build/publish images to a registry (Docker Hub / GitHub Container Registry).

---

## Docker Compose sample usage for updates
To upgrade a running container to a new image/tag:
```bash
# pull new image (if remote) or rebuild locally
docker compose pull ingestion-service || docker compose build ingestion-service
# stop and remove the service container
docker compose up -d --no-deps --build ingestion-service
# or for full restart
docker compose down && docker compose up -d
```


