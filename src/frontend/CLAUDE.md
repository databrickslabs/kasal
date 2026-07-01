# Frontend CLAUDE.md

Frontend-specific instructions for Claude Code when working in the frontend directory.

## Commands

### Development (Vite-based)
- **Install dependencies**: `npm install`
- **Start dev server**: `npm start` (alias for `vite`, runs on http://localhost:3000)
- **Build for production**: `npm run build` (`tsc -b && vite build`, outputs to `dist/`)
- **Run tests**: `npm test` (Vitest) or `npm run test:run` (single run)
- **Lint code**: `npm run lint` (ESLint)
- **Type check**: `npm run tsc`

## Architecture

### Technology Stack
- React 18 with TypeScript, bundled with **Vite** (not Create React App)
- Material-UI (MUI) for components
- ReactFlow for workflow visualization
- **Zustand** for state management (primary; some legacy Redux Toolkit still present)
- Axios for HTTP requests, wrapped by `apiClient`
- **Vitest** + React Testing Library for tests (no Cypress in this repo)

### Directory Structure
```
src/
├── components/      # UI components by feature
├── store/           # Zustand state management
├── api/             # API service layer
├── types/           # TypeScript definitions
└── config/
    └── api/         # API configuration
```

## Development Patterns

### API Configuration
- **ALWAYS use `apiClient`** from `src/config/api/ApiConfig.ts` for backend communication
- Frontend services should use static methods and `apiClient` for HTTP requests
- Do NOT use the legacy `ApiService`

### TypeScript Patterns
- Strong typing for all API responses and requests
- Use generic types: `apiClient.get<ResponseType>()`
- Define interfaces in `types/` directory

### State Management
- Use Zustand stores for global state
- Store files in `store/` directory
- Follow existing store patterns

### Component Guidelines
- Use functional components with hooks
- Follow Material-UI theming
- Keep components focused and single-purpose
- Extract reusable logic into custom hooks

## Documentation Management

User-facing docs live in `src/docs/` (project-wide rule). The copy to
`public/docs/` is **automated by `src/build.py`** at build time (it recursively
copies `.md` + image assets), so you do not copy files by hand. When adding a doc
that should appear in the in-app Documentation viewer, update the `docSections`
array in `src/components/Documentation/Documentation.tsx`.

## Testing Strategy (Vitest)

### Test Types
- **Component Tests**: React Testing Library
- **Hook Tests**: Custom hooks testing
- Test files are colocated as `*.test.ts(x)` next to the code they cover

### Testing Commands
- Run all tests (watch): `npm test`
- Run once (CI): `npm run test:run`
- Run with coverage: `npm run test:run -- --coverage`
- Run a single file: `npm run test:run -- src/path/File.test.tsx`

## Workflow Editor

### ReactFlow Integration
- Visual workflow designer component
- Located in `components/Workflow/`
- Handles node creation, connection, and editing
- Integrates with Zustand store for state management

## Build Process

### Production Build
- Run `npm run build` (`tsc -b && vite build`) to create an optimized build
- Output goes to the **`dist/`** directory (Vite default)
- `src/build.py` copies `dist/` to `../../frontend_static/` for deployment

## Critical Rules

- **DO NOT restart frontend service** - It uses hot module replacement (HMR)
- **Check service status**: `ps aux | grep "npm start"`
- **NEVER commit without running**: `npm run lint` and `npm run tsc`
- Use Material-UI components consistently
- Follow existing component patterns

## Environment

- Node.js 18+ recommended
- React 18 with TypeScript, Vite dev server (HMR)
- Development server auto-refreshes on file changes
- Environment variables use Vite's `VITE_` prefix (e.g. `VITE_API_URL`); `.env` is not committed