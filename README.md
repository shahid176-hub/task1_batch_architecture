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
- ingestion-service/        # ingestion microservice (python/fastapi )
- batch-processing/         # spark job / scripts 
- minio/                    # MinIO config / helpful scripts
- delivery-api/             # API to serve aggregated results 
- analytics/                # notebooks / analysis
- airflow/                  # DAG  for scheduling
- infra/                    # docker-compose, env files, docs
- .github/workflows/        # CI workflow example

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


