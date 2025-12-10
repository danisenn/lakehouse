# Docker Deployment Guide

This guide explains how to run the lakehouse assistant with Docker in different environments.

## Quick Start

### Local Development (Hybrid: Docker Backend + Local Frontend)

```bash
# Terminal 1: Run only backend services with Docker
docker compose -f docker-compose.local.yml up

# Terminal 2: Run frontend locally for hot reload
cd frontend
npm run dev  # Frontend at http://localhost:5173
```

**Ports:**
- Backend: `http://localhost:8000`
- Frontend: `http://localhost:5173` (Vite dev server)
- Ollama: `http://localhost:11434`

---

### Server/Production (Full Docker)

```bash
# Use the server configuration
docker compose -f docker-compose.server.yml up -d

# Or use the default docker-compose.yml (same as server)
docker compose up -d
```

**Ports:**
- Backend: `http://localhost:8889`
- Frontend: `http://localhost:8888`
- Ollama: `http://localhost:11434`

---

## Configuration Files

| File | Purpose | Backend Port | Frontend Port |
|------|---------|--------------|---------------|
| `docker-compose.local.yml` | Local dev (backend only) | 8000 | N/A (run via npm) |
| `docker-compose.server.yml` | Server/production (full stack) | 8889 | 8888 |
| `docker-compose.yml` | Default (same as server) | 8889 | 8888 |

---

## Environment Variables

### Backend

Set in `docker-compose.yml` or `.env` file:

```bash
CORS_ORIGINS=http://localhost:5173,http://localhost:8888
ARTIFACT_DIR=/app/artifacts
OLLAMA_HOST=http://ollama:11434
```

### Frontend

Set in `docker-compose.yml`:

```yaml
environment:
  - VITE_API_URL=http://localhost:8889
```

---

## Common Commands

### Start Services

```bash
# Local dev
docker compose -f docker-compose.local.yml up

# Server/production
docker compose -f docker-compose.server.yml up -d

# View logs
docker compose logs -f backend
docker compose logs -f frontend
```

### Stop Services

```bash
docker compose down

# With specific config
docker compose -f docker-compose.local.yml down
```

### Rebuild After Changes

```bash
# Rebuild specific service
docker compose build backend
docker compose build frontend

# Rebuild and restart
docker compose up -d --build
```

---

## Data Persistence

Volumes are automatically mounted:

```yaml
volumes:
  - ./backend/src:/app/src           # Backend source code (hot reload)
  - ./backend/data:/app/data         # Data files
  - ./backend/artifacts:/app/artifacts  # Generated artifacts
  - ollama_data:/root/.ollama        # Ollama models
```

**Add datasets:** Copy files to `backend/data/`

---

## Switching Between Environments

### On Your Local Machine

```bash
# Use local development setup
docker compose -f docker-compose.local.yml up
cd frontend && npm run dev
```

### On Your Server

```bash
# Pull latest code
git pull

# Use server configuration
docker compose -f docker-compose.server.yml up -d --build
```

**No configuration file conflicts** - just use different compose files!

---

## Troubleshooting

**Port already in use?**
```bash
# Find what's using the port
lsof -i :8889

# Use different compose file
docker compose -f docker-compose.local.yml up
```

**Frontend can't connect to backend?**
- Check backend is running: `curl http://localhost:8889/api/v1/health`
- Check nginx proxy in frontend container logs
- Verify CORS_ORIGINS includes your frontend URL

**Data not showing up?**
- Verify data directory is mounted: `docker compose exec backend ls -la /app/data`
- Check backend logs: `docker compose logs backend`
