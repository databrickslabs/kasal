# Getting Started with Kasal

This guide will help you set up and run the Kasal platform on your local machine for development purposes.

## Prerequisites

- **Python 3.9 or higher** for the backend
- **Node.js 16 or higher** for the frontend
- **Poetry** for Python dependency management
- **npm** for frontend dependency management

## Project Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/kasal.git
cd kasal
```

### 2. Backend Setup

#### Install Dependencies

```bash
cd backend
poetry install
```

#### Environment Configuration

Create a `.env` file in the backend directory:

```bash
cp .env.example .env
```

Edit the `.env` file to configure:

- Database settings (SQLite by default for development)
- API keys for any integrated LLM services
- Other environment-specific settings

#### Initialize the Database

Run database migrations:

```bash
poetry run alembic upgrade head
```

The application is configured to automatically seed the database with initial data on startup. If you want to disable this, set `AUTO_SEED_DATABASE=false` in your `.env` file.

#### Start the Backend Server

```bash
# Either use the provided script
./run.sh

# Or start with uvicorn directly
poetry run uvicorn src.main:app --reload
```

The backend API will be available at http://localhost:8000.

### 3. Frontend Setup

#### Install Dependencies

```bash
cd ../frontend
npm install
```

#### Start the Frontend Development Server

```bash
npm start
```

The frontend will be available at http://localhost:3000.

## Building the Documentation

Kasal uses MkDocs with the Material theme for documentation. To build and view the documentation locally:

### 1. Install MkDocs and the Material theme

```bash
pip install mkdocs mkdocs-material
```

### 2. Build the documentation

```bash
# From the project root
mkdocs build
```

This will generate the documentation site in the `docs/site` directory.

### 3. Serve the documentation locally

```bash
# From the project root
mkdocs serve
```

This will start a local server at http://127.0.0.1:8000/ where you can view the documentation.

### 4. Editing Documentation

Documentation files are written in Markdown and located in the `docs/` directory. To update the Getting Started guide:

1. Edit the `docs/GETTING_STARTED.md` file
2. Run `mkdocs serve` to preview changes in real-time
3. Once satisfied, commit your changes to the repository

The backend server will automatically serve the documentation at http://localhost:8000/docs when running.

## Accessing the Application

Once both the backend and frontend are running, you can access:

- **Kasal Web Interface**: http://localhost:3000
- **API Documentation**: http://localhost:8000/api-docs
- **Project Documentation**: http://localhost:8000/docs

## Project Structure

### Backend (FastAPI + SQLAlchemy)

```
backend/
├── src/                 # Application source code
│   ├── api/             # API routes and controllers
│   ├── core/            # Core functionality and base classes
│   ├── db/              # Database configuration and models
│   ├── models/          # SQLAlchemy data models
│   ├── repositories/    # Data access layer
│   ├── schemas/         # Pydantic models for validation
│   ├── services/        # Business logic services
│   ├── engines/         # CrewAI integration
│   ├── seeds/           # Database seeders
│   ├── config/          # Configuration management
│   ├── utils/           # Utility functions
│   ├── main.py          # Application entry point
└── tests/               # Test suite
```

### Frontend (React + TypeScript)

```
frontend/
├── src/                 # Application source code
│   ├── components/      # Reusable UI components
│   ├── pages/           # Page components
│   ├── hooks/           # Custom React hooks
│   ├── services/        # API services
│   ├── store/           # State management with Zustand
│   ├── types/           # TypeScript type definitions
│   ├── utils/           # Utility functions
└── public/              # Static assets
```

## Using Kasal

### Creating Your First Agent Workflow

1. Access the Kasal web interface at http://localhost:3000
2. Navigate to the workspace page
3. Use the visual designer to:
   - Create agents by dragging them onto the canvas
   - Define agent properties, tools, and memory
   - Connect agents to create workflows
   - Configure task dependencies

### Running Your Workflow

1. Save your workflow
2. Click the "Execute" button to run the workflow
3. Monitor execution in real-time
4. View results in the execution panel

## Development Guidelines

- **Backend Development**: Follow the clean architecture pattern with clear separation between layers
- **API Design**: Use RESTful principles for all endpoints
- **Database Changes**: Create migrations using Alembic for any model changes
- **Frontend Development**: Use TypeScript for all components and maintain a consistent UI design
- **Testing**: Write tests for all new features

## Troubleshooting

### Common Issues

- **Database Errors**: Verify your database configuration in `.env`
- **Connection Refused**: Ensure both backend and frontend servers are running
- **Authentication Errors**: Check API keys and credentials in `.env`
- **Frontend Build Errors**: Clean node_modules and reinstall dependencies

### Getting Help

If you encounter issues:

1. Check the logs in `backend/logs/`
2. Review the API documentation at http://localhost:8000/api-docs
3. Consult the comprehensive documentation at http://localhost:8000/docs

## Next Steps

- Learn about [Kasal Architecture](ARCHITECTURE.md)
- Explore the [CrewAI Engine](CREWAI_ENGINE.md) integration
- Understand [Frontend State Management](zustand.md) 