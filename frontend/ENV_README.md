# Environment Configuration Files

This directory contains environment-specific configuration files for the frontend.

## Files

- **`.env.local`** - Used for local development (automatically loaded by Vite)
- **`.env.production`** - Used for production/server deployment
- **`.env.example`** - Template showing available environment variables (should be committed to git)

## Usage

### Local Development

When running `npm run dev` locally, Vite automatically loads `.env.local`:

```bash
# .env.local (not committed to git)
VITE_BACKEND_URL=http://localhost:8000
```

### Server/Production Deployment

When building for production or running on the server, create `.env.production`:

```bash
# .env.production (not committed to git)
VITE_BACKEND_URL=http://your-server:8889
```

Or set the environment variable directly:

```bash
VITE_BACKEND_URL=http://your-server:8889 npm run dev
```

## Available Variables

- **`VITE_BACKEND_URL`** - Backend API URL for dev proxy (defaults to `http://localhost:8000`)
- **`VITE_API_URL`** - Optional: Direct API URL to bypass proxy (for production builds)

## Priority Order

Vite loads environment files in this priority (highest to lowest):
1. `.env.local` (local overrides, never committed)
2. `.env.production` or `.env.development` (mode-specific)
3. `.env` (shared defaults)

**Note:** All `.env.local` and `.env.*.local` files are ignored by git via `.gitignore`.
