import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite configuration with a dev proxy so the frontend can call the backend
// using same-origin relative paths like "/api/..." without hard-coding hosts.
//
// By default, we proxy /api to http://localhost:8000. You can change the target
// if your backend runs elsewhere, or set VITE_API_URL to bypass the proxy.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        // If your backend does not expect the /api prefix, you can uncomment the next line
        // and rewrite it. Our backend appears to serve under /api already, so keep as is.
        // rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
