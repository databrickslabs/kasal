# Kasal

**Build intelligent AI agent workflows with visual simplicity and enterprise power.**

[![YouTube Video](https://img.youtube.com/vi/0d5e5rSe5JI/0.jpg)](https://www.youtube.com/watch?v=0d5e5rSe5JI)

Kasal transforms complex AI orchestration into an intuitive visual experience. Design, deploy, and monitor autonomous AI agents that collaborate seamlessly to solve real-world business challenges.

## Why Kasal?

• **Visual Workflow Designer** - Drag-and-drop interface for creating sophisticated agent interactions  
• **Enterprise-Ready** - Built for Databricks with OAuth, security, and scalability  
• **Extensible Toolkit** - Rich library of tools including Genie, custom APIs, and data connectors  
• **Real-time Monitoring** - Live execution tracking with detailed logs and performance insights  
• **Production-Grade** - Robust error handling, retry logic, and enterprise deployment patterns

## What You Can Build

- **Data Analysis Pipelines** - Agents that query, analyze, and visualize your data
- **Content Generation Systems** - Collaborative agents for research, writing, and content creation  
- **Business Process Automation** - Intelligent workflows that adapt and make decisions
- **Customer Support Bots** - Multi-agent systems with specialized knowledge domains
- **Research & Development** - Agents that gather, synthesize, and present insights

## Get Started in Minutes

### **Databricks Marketplace** (Recommended)
Install directly from the Databricks Apps Marketplace with one click. Perfect for production use with automatic updates and enterprise support.

### **Deploy from Source**
Use the deployment script in this codebase for custom installations and development. Ideal for customization and advanced configurations.

### **Local Development**
Quick setup for testing and development - requires Python 3.9+ and Node.js.

## See It in Action

![Kasal UI Screenshot](src/docs/images/kasal-ui-screenshot.png)
*Visual workflow designer for creating AI agent collaborations*

Create your first agent workflow in under 2 minutes:
1. **Design** - Drag agents onto the canvas and define their roles
2. **Connect** - Link agents to create collaboration flows  
3. **Execute** - Hit run and watch your agents work together
4. **Monitor** - View real-time logs and execution traces

---

## Documentation

| Topic | Description |
|-------|-------------|
| **[Getting Started](src/docs/GETTING_STARTED.md)** | Complete setup guide for development and deployment |
| **[Build & Deploy](src/docs/BUILD.md)** | Building frontend and deployment instructions |
| **[Deployment Guide](src/docs/DEPLOYMENT_GUIDE.md)** | Databricks Apps deployment with OAuth configuration |
| **[Architecture](src/docs/ARCHITECTURE.md)** | System architecture and design patterns |
| **[CrewAI Engine](src/docs/CREWAI_ENGINE.md)** | AI agent orchestration engine documentation |
| **[Database Migrations](src/docs/DATABASE_MIGRATIONS.md)** | Database schema management with Alembic |
| **[Models & Schemas](src/docs/MODELS.md)** | SQLAlchemy models and Pydantic schemas |
| **[Repository Pattern](src/docs/REPOSITORY_PATTERN.md)** | Data access layer implementation |
| **[LLM Manager](src/docs/LLM_MANAGER.md)** | Multi-provider LLM configuration and management |
| **[Embeddings](src/docs/EMBEDDINGS.md)** | Documentation embeddings for enhanced crew generation |

### More Documentation
- **[API Documentation](src/docs/)** - Complete API reference
- **[Best Practices](src/docs/BEST_PRACTICES.md)** - Development guidelines
- **[Security Model](src/docs/SECURITY_MODEL.md)** - Authentication and authorization
- **[Testing Guide](src/backend/tests/README.md)** - Testing strategy and coverage

---

## Architecture

Kasal uses a modern, layered architecture designed for scalability and maintainability:

**Frontend (React)** → **API (FastAPI)** → **Services** → **Repositories** → **Database**

The CrewAI Engine integrates at the service layer for intelligent agent orchestration.

## License

Licensed under the [Databricks License](src/LICENSE)

---

## Additional Resources

[Unlocking Databricks Marketplace: A Hands-On Guide for Data Consumers](https://www.databricks.com/dataaisummit/session/unlocking-databricks-marketplace-hands-guide-data-consumers)