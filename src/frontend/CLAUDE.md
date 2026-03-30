# Frontend CLAUDE.md

Frontend-specific instructions for Claude Code when working in the frontend directory.

## Commands

### Development
- **Install dependencies**: `npm install`
- **Start dev server**: `npm start` (runs on http://localhost:3000)
- **Build for production**: `npm run build`
- **Run tests**: `npm test`
- **Lint code**: `npm run lint`
- **Type check**: `npm run tsc`

## Architecture

### Technology Stack
- React 18 with TypeScript
- Material-UI (MUI) for components
- ReactFlow for workflow visualization
- Zustand for state management (migrated from Redux)
- Axios for HTTP requests

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

When adding new documentation:
1. Copy `.md` files from `../../docs/` to `public/docs/`
2. Update `docSections` array in `src/components/Documentation/Documentation.tsx`

## Testing Strategy

### Test Types
- **Component Tests**: React Testing Library
- **Hook Tests**: Custom hooks testing
- **E2E Tests**: Cypress for user workflows

### Testing Commands
- Run all tests: `npm test`
- Run with coverage: `npm test -- --coverage`
- Run specific test: `npm test -- --testNamePattern="TestName"`

## Workflow Editor

### ReactFlow Integration
- Visual workflow designer component
- Located in `components/Workflow/`
- Handles node creation, connection, and editing
- Integrates with Zustand store for state management

## Build Process

### Production Build
- Run `npm run build` to create optimized production build
- Output goes to `build/` directory
- Static assets are copied to `../../frontend_static/` for deployment

## Critical Rules

- **DO NOT restart frontend service** - It uses hot module replacement (HMR)
- **Check service status**: `ps aux | grep "npm start"`
- **NEVER commit without running**: `npm run lint` and `npm run tsc`
- Use Material-UI components consistently
- Follow existing component patterns

## Environment

- Node.js 16+ required
- React 18 with TypeScript
- Development server auto-refreshes on file changes
- Environment variables in `.env` file (not committed)