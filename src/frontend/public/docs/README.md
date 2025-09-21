# Kasal Documentation Hub

**Enterprise AI Agent Orchestration Platform**

---

## Quick Start

Choose your documentation based on your role:

## Business Documentation

### [Business User Guide](./BUSINESS_USER_GUIDE.md)
**For Leaders, Managers, and Business Users**

Get started with Kasal without any technical knowledge. Learn how to create AI workflows, calculate ROI, and manage your AI workforce.

**Key Topics:**
- ROI Calculator and Cost Analysis
- Success Stories and Case Studies
- 15-Minute Quick Setup Guide
- Managing AI Workflows Without Code

---

## Technical Documentation

### [Developer Guide](./DEVELOPER_GUIDE.md)
**For Software Engineers and Developers**

Build and integrate with Kasal's powerful AI orchestration platform. From quick starts to advanced implementations.

**Key Topics:**
- 30-Second Quick Start with Code Examples
- API Integration and SDK Usage
- Testing Strategies and Best Practices
- Production Deployment Guides

### [API Reference](./API_REFERENCE.md)
**For API Integrators and Backend Engineers**

Complete REST API documentation with examples, WebSocket events, and SDK libraries.

**Key Topics:**
- RESTful Endpoints Documentation
- WebSocket Real-time Events
- Authentication and Authorization
- Rate Limits and Error Handling

---

## Architecture Documentation

### [Solution Architecture Guide](./ARCHITECTURE_GUIDE.md)
**For Solution Architects and Technical Leaders**

Understand Kasal's architecture, design patterns, and enterprise integration strategies.

**Key Topics:**
- System Design and Components
- Security Model and Compliance
- Scaling Strategies and Performance
- Integration Patterns and Best Practices

---

## Getting Started in 30 Seconds

```bash
# Clone the repository
git clone https://github.com/databrickslabs/kasal

# Install dependencies
cd kasal && pip install -r src/requirements.txt

# Start the backend
cd src/backend && ./run.sh

# In another terminal, start the frontend
cd src/frontend && npm install && npm start
```

Access the application at `http://localhost:3000`

---

## Key Features

| Feature | Description |
|---------|-------------|
| **AI Agent Orchestration** | Create and manage teams of AI agents working together |
| **Visual Workflow Designer** | Drag-and-drop interface for creating complex workflows |
| **Enterprise Security** | SOC2 compliant with role-based access control |
| **Databricks Integration** | Native integration with Databricks platform |
| **Scalable Architecture** | Microservices-ready, horizontally scalable design |
| **Multi-Model Support** | Support for GPT-4, Claude, Databricks models, and more |

---

## Documentation Structure

```
docs/
├── README.md                    # This file - Documentation hub
├── BUSINESS_USER_GUIDE.md       # Business user documentation
├── DEVELOPER_GUIDE.md           # Developer documentation
├── ARCHITECTURE_GUIDE.md        # Architecture documentation
├── API_REFERENCE.md             # API reference
└── archive/                     # Legacy documentation
    ├── technical/               # Technical deep-dives
    ├── security/                # Security documentation
    └── guides/                  # Various guides
```

---

## Technology Stack

- **Backend**: FastAPI, SQLAlchemy 2.0, Python 3.9+
- **Frontend**: React 18, TypeScript, Material-UI
- **AI Engine**: CrewAI, LangChain
- **Database**: PostgreSQL / SQLite
- **Authentication**: JWT + Databricks OAuth

---

## Support and Resources

### Getting Help

- **GitHub Issues**: [github.com/databrickslabs/kasal/issues](https://github.com/databrickslabs/kasal/issues)
- **Email**: Contact your Databricks support team
- **Documentation**: You're already here!

### Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/databrickslabs/kasal/blob/main/CONTRIBUTING.md) for details.

### License

This project is licensed under the Apache License 2.0 - see the [LICENSE](https://github.com/databrickslabs/kasal/blob/main/LICENSE) file for details.

---

## Version Information

- **Current Version**: 2.0.0
- **Documentation Updated**: January 2025
- **Minimum Python Version**: 3.9
- **Minimum Node Version**: 16.0

---

*Built with ❤️ by Databricks Labs*