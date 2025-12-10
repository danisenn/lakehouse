import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite configuration with a dev proxy so the frontend can call the backend
// using same-origin relative paths like "/api/..." without hard-coding hosts.
//
// The proxy target can be configured via VITE_API_URL environment variable.
// Default: http://localhost:8000 (for local development)
// Production: Set VITE_API_URL in .env.production or via environment
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: process.env.VITE_API_URL || 'http://localhost:8000',
        changeOrigin: true,
        // If your backend does not expect the /api prefix, you can uncomment the next line
        // and rewrite it. Our backend appears to serve under /api already, so keep as is.
        // rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
