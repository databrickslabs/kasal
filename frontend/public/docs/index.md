# Kasal Documentation

Welcome to the Kasal documentation! Kasal is an AI agent workflow orchestration platform built with FastAPI and CrewAI.

## What is Kasal?

Kasal is a backend API system for creating, managing, and executing AI agent workflows. It provides:

- **REST API** for managing AI agents, crews, and tasks
- **CrewAI Engine Integration** for autonomous AI agent execution
- **Multi-LLM Support** (OpenAI, Anthropic, DeepSeek, Ollama, Databricks)
- **Databricks Apps Deployment** with OAuth integration
- **Real-time Execution Monitoring** with detailed logging and tracing

## Quick Start

1. **Installation**: See [Getting Started](GETTING_STARTED.md) for setup instructions
2. **Architecture**: Learn about the [system architecture](ARCHITECTURE.md)
3. **API Usage**: Explore the [REST API documentation](API.md)
4. **Deployment**: Deploy to [Databricks Apps](DEPLOYMENT_GUIDE.md)

## Core Components

- **Agents**: Individual AI workers with specific roles and capabilities
- **Crews**: Teams of agents working together on complex tasks
- **Tasks**: Specific work items for agents to complete
- **Executions**: Running workflows with real-time monitoring

## Documentation Sections

Use the sidebar to navigate through:

- **Getting Started** - Installation and setup
- **Architecture** - System design and patterns  
- **Backend Features** - CrewAI engine, LLM management, logging
- **API & Usage** - REST endpoints and keyboard shortcuts
- **Deployment** - Databricks Apps deployment guide

## Getting Help

- Check the logs in `backend/logs/` for troubleshooting
- Review the [API documentation](API.md) for endpoint details
- Consult the specific guides in the documentation sections

---

*Kasal - AI Agent Workflow Orchestration Platform*