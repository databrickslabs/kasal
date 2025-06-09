# Welcome to Kasal

## Quick Navigation

- **[Getting Started](GETTING_STARTED.md)** - Get up and running quickly with installation and configuration guides.
- **[Generation Features](generation.md)** - Learn how to use AI to generate agents, tasks, and crew plans.
- **[Architecture](ARCHITECTURE.md)** - Understand Kasal's clean architecture and design principles.
- **[CrewAI Engine](CREWAI_ENGINE.md)** - Learn about Kasal's integration with CrewAI for agent-based workflows.
- **[Frontend](zustand.md)** - Explore the React frontend and state management with Zustand.

## What is Kasal?

Kasal is a low-code/no-code platform that makes it easy for non-technical users to build, orchestrate, and deploy agentic AI solutions. By providing a visual interface on top of the powerful CrewAI framework, Kasal democratizes access to AI agent technology, allowing anyone to create sophisticated AI workflows without writing code.

At its core, Kasal abstracts away the complexity of the CrewAI framework, providing an intuitive interface where users can visually design, connect, and deploy autonomous AI agents that work together to solve complex problems.

### Core Functionality

Kasal empowers non-technical users to:

1. **Design AI Agent Workflows Visually**: Create multi-agent systems through an intuitive drag-and-drop interface, eliminating the need for coding knowledge.

2. **Orchestrate Agent Interactions**: Define communication patterns and collaboration methods between agents using visual connectors and simple configuration panels.

3. **Monitor Executions in Real-Time**: Track your AI agents' progress and outputs through comprehensive dashboards and visualizations.

4. **Connect to External Tools**: Integrate with external services and data sources through pre-built connectors, no API expertise required.

5. **Deploy with One Click**: Move from design to deployment seamlessly with simplified deployment options.

### Technical Foundation

While hiding complexity for end users, Kasal is built on a robust technical foundation:

- **Backend**: A FastAPI application with fully asynchronous request handling, organized in a clean, layered architecture.
- **Database**: SQLAlchemy 2.0 with async support, implementing the repository pattern for data access.
- **Frontend**: A modern React application with Zustand state management and a responsive UI.
- **API**: RESTful API design with comprehensive OpenAPI documentation.
- **Deployment**: Built as a Python wheel package that can be easily installed and deployed.

## Architecture Overview

Kasal follows a modular, layered architecture that separates concerns and promotes maintainability:

```
┌─────────────────────────────────────────────────┐
│                  Frontend Layer                 │
│                                                 │
│  React Components + Zustand State Management    │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│                   API Layer                     │
│                                                 │
│  FastAPI Routes + OpenAPI Documentation         │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│                 Service Layer                   │
│                                                 │
│  Business Logic + CrewAI Engine Integration     │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│               Repository Layer                  │
│                                                 │
│  Data Access + SQLAlchemy ORM                   │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│                Database Layer                   │
│                                                 │
│  SQLite/PostgreSQL + Migrations                 │
└─────────────────────────────────────────────────┘
```

## CrewAI Integration

Kasal simplifies the use of CrewAI, an advanced framework for orchestrating autonomous AI agents. While CrewAI is powerful, it typically requires Python programming knowledge. Kasal provides:

### No-Code Agent Creation

- **Visual Agent Builder**: Create specialized agents with specific roles, goals, and backstories through a simple form interface
- **Pre-configured Tools**: Select from a library of ready-to-use tools without writing code
- **Template Library**: Start with pre-built agent templates for common use cases

### Visual Workflow Design

- **Drag-and-Drop Interface**: Design complex agent workflows visually
- **Workflow Templates**: Use pre-built workflow patterns for common scenarios
- **Visual Dependency Mapping**: Create task dependencies with simple connector lines

### One-Click Deployment

- **Execution Dashboard**: Launch and monitor agent executions from a central dashboard
- **Result Visualization**: View and analyze results through intuitive visualizations
- **Export Capabilities**: Export results in various formats for further use

## Key Features

- **Visual Workflow Designer**: Build agent workflows without coding
- **Template Library**: Start quickly with pre-built agents and workflows
- **Integration Marketplace**: Connect to external services without API knowledge
- **Real-Time Monitoring**: Track agent activities through visual dashboards
- **User-Friendly Interface**: Designed for non-technical users
- **Enterprise Security**: Role-based access control and data protection
- **Scalable Architecture**: Handle workflows of any complexity

## Documentation Structure

Our documentation is organized following best practices to help you find what you need:

- **Getting Started**: Installation and quick setup guides
- **Architecture**: System design and architectural patterns
- **Features**: Detailed documentation of key features
- **Development**: Guidelines for developers contributing to Kasal

## Quick Links

| Resource | Description |
| --- | --- |
| [Installation Guide](GETTING_STARTED.md) | Step-by-step instructions to set up Kasal |
| [Generation Features](generation.md) | Guide to AI-assisted agent, task, and crew generation |
| [Architecture Overview](ARCHITECTURE.md) | High-level overview of Kasal's architecture |
| [CrewAI Engine](CREWAI_ENGINE.md) | Details on the CrewAI integration |
| [Best Practices](BEST_PRACTICES.md) | Recommended practices for working with Kasal |
| [API Reference](MODELS.md) | Technical reference for Kasal's API |

## Contribution

We welcome contributions to Kasal! If you'd like to contribute, please review our [Best Practices](BEST_PRACTICES.md) guide and submit a pull request.

---

<small>
Kasal Documentation • Built with [MkDocs](https://www.mkdocs.org/) and [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/)
</small> 