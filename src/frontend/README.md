# Kasal frontend

React and TypeScript, built with Vite. Run commands from `src/frontend` after
installing dependencies with `npm install`.

| Command | Purpose |
| --- | --- |
| `npm start` or `npm run dev` | Development server on port 3000 |
| `npm run build` | TypeScript checks and production output in `dist/` |
| `npm run preview` | Serve the existing production build locally |
| `npm test` | Vitest in watch mode |
| `npm run test:run` | Run tests once |
| `npm run lint` | ESLint using `eslint.config.js` |
| `npm run build:analyze` | Build with the bundle visualizer |

The API client reads `VITE_API_URL`. It defaults to
`http://localhost:8000/api/v1` in development and `/api/v1` in production.
Use `.env.local` for local overrides. Deployment sets this value explicitly.

Application documentation belongs in `../docs/`. Vite stages it recursively,
including images, JSON examples and CSS, alongside `public/` assets in the
ignored `.generated/public/` directory. The app serves these pages at `/docs/`.
Edit the source documentation and restart Vite to refresh the staged copy;
do not edit generated files.

To prepare `src/frontend_static/` for deployment or wheel packaging, run
`npm run build` from `src/`. This installs frontend dependencies, builds the
frontend and publishes the complete `dist/` snapshot. `src/build.py` uses the
same lifecycle.

Views live under `src/features`, while `src/app` owns routes and cross-feature
workspace composition. `src/shared/api` owns the single HTTP transport; domain
clients remain under `src/api`, and shared contracts under `src/types`.
Tests stay beside their source. See the [code structure guide](../docs/CODE_STRUCTURE_GUIDE.md)
for backend ownership and import conventions.
