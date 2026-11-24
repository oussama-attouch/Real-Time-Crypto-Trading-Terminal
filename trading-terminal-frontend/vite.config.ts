import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],

  /**
   * REQUIRED — without this the page is black and the console shows:
   *   Uncaught ReferenceError: global is not defined
   *
   * sockjs-client evaluates a bare `global` identifier at *import* time, e.g.
   *   location.js:3  module.exports = global.location || { ... }
   *   entry.js:8     if ('_sockjs_onload' in global) { ... }
   *
   * `global` is a Node/CommonJS concept; the browser has `globalThis` and `window`.
   * Because main.tsx imports tradeStream -> wsService -> sockjs-client, that throw
   * happens while the module graph is still evaluating, so `createRoot().render()`
   * never runs and nothing mounts.
   *
   * Note this cannot be caught by the toolchain: `tsc` only type-checks and
   * `vite build` only transforms — neither evaluates the module.
   */
  define: {
    global: 'globalThis',
  },

  server: {
    port: 5173,
    proxy: {
      // `ws: true` for the raw WebSocket transport; the SockJS HTTP fallbacks
      // (info / xhr_streaming / xhr) ride the same prefix as ordinary HTTP.
      '/ws-trades': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        ws: true,
      },
      // Reserved for the REST surface (orders, history). No endpoint exists yet.
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
});
