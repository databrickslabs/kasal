

# Kasal Documentation Hub

**Enterprise AI Agent Orchestration Platform**

---

## Quick Start
Find the right documentation for your role and get productive fast.

Choose your documentation based on your role:


## Technical Documentation
Developer-focused guides, code structure, and API reference.
### [Code structure Guide](./CODE_STRUCTURE_GUIDE.md)
**For Software Engineers and Developers**

Build and integrate with Kasal's powerful AI orchestration platform. From quick starts to advanced implementations.

**Key Topics:**
- High level code structure
- Different folders and underlying components

---

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
System design, patterns, and enterprise integration guidance.

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
One command sequence to run both backend and frontend locally.

```bash
# Clone the repository
git clone https://github.com/databrickslabs/kasal

# Start the backend (uv syncs dependencies automatically)
cd kasal/src/backend && ./run.sh

# In another terminal, start the frontend
cd kasal/src/frontend && npm install && npm start
```

Access the application at `http://localhost:3000`

---

## Key Features
What Kasal provides out of the box to build, operate, and govern AI workflows.

| Feature | Description |
|---------|-------------|
| **AI Agent Orchestration** | Create and manage teams of AI agents working together |
| **Visual Workflow Designer** | Drag-and-drop interface for creating complex workflows |
| **Enterprise Security** | SOC2 compliant with role-based access control |
| **Databricks Integration** | Native integration with Databricks platform |
| **Scalable Architecture** | Microservices-ready, horizontally scalable design |
| **Multi-Model Support** | Support for GPT-4, Claude, Databricks models, and more |

---

## Power BI → Unity Catalog Metric View Migration
End-to-end automation of PBI semantic model translation to UC Metric Views.

### [UCMV Pipeline Config Guide](./UCMV_PIPELINE_CONFIG_GUIDE.md)
**For Data Engineers and Analytics Engineers**

Explains every config key in the pipeline — what is auto-extracted from PBI APIs and what requires human domain knowledge.

**Key Topics:**
- Auto-extracted fields (`relationships`, `measures`, `mquery`, `scan_data`)
- Manual fields (`join_key_map`, `filter_sets`, `switch_decompositions`, `measure_resolutions`)
- When and why human review is needed

### [PowerBI Tools Reference](./powerbi/README.md)
**For Platform Engineers and Power BI Admins**

Full reference for all 18 Power BI tools (Tools 72–90) and their configuration.

---

## Example Crews & Flows
Import-ready JSON definitions for the full UCMV migration pipeline.

### [Examples Folder](./examples/)

| File | Description |
|------|-------------|
| [`crew_ucmv_pipeline_config_generator.json`](./examples/crew_ucmv_pipeline_config_generator.json) | **Crew 1** — Connects to PBI REST API, extracts metadata, proposes pipeline config |
| [`crew_uc_metric_view_generator.json`](./examples/crew_uc_metric_view_generator.json) | **Crew 2** — Translates DAX → Spark SQL, generates UC Metric View YAML + SQL |
| [`crew_ucmv_quality_validator.json`](./examples/crew_ucmv_quality_validator.json) | **Crew 3** — Validates every measure's translation (VALID/EQUIVALENT/REVIEW/INVALID) |
| [`flow_ucmv_plus_validation.json`](./examples/flow_ucmv_plus_validation.json) | **Full flow** — Chains all 3 crews end-to-end |
| [`crew_pbi_analyst_qa.json`](./examples/crew_pbi_analyst_qa.json) | **Analytics Q&A** — 3-agent crew: fetch → reduce → DAX (natural language questions against live PBI model) |

**How to import**: Kasal UI → Crews (or Flows) → Import → select the JSON file.
All credentials are placeholders — see the [examples README](./examples/README.md) for the full setup guide.

---

## Documentation Structure
How this folder is organized and where to find topics.

```
docs/
├── README.md                         # This file — Documentation hub
├── UCMV_PIPELINE_CONFIG_GUIDE.md     # Pipeline config reference
├── CODE_STRUCTURE_GUIDE.md           # Code structure documentation
├── DEVELOPER_GUIDE.md                # Developer documentation
├── ARCHITECTURE_GUIDE.md             # Architecture documentation
├── examples/                         # Import-ready crew & flow JSONs
│   ├── README.md                     # Setup guide for UCMV examples
│   ├── crew_ucmv_pipeline_config_generator.json
│   ├── crew_uc_metric_view_generator.json
│   ├── crew_ucmv_quality_validator.json
│   └── flow_ucmv_plus_validation.json
└── archive/                          # Legacy documentation
    ├── technical/
    ├── security/
    └── guides/
```

---

## Technology Stack
Core frameworks and platforms used across the project.

- **Backend**: FastAPI, SQLAlchemy 2.0, Python 3.9+
- **Frontend**: React 18, TypeScript, Material-UI
- **AI Engine**: CrewAI, LangChain
- **Database**: PostgreSQL / SQLite
- **Authentication**: JWT + Databricks OAuth

---

## Support and Resources
Where to get help and how to contribute.

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
Project and documentation version details.

- **Current Version**: 2.0.0
- **Documentation Updated**: December 2025
- **Minimum Python Version**: 3.10
- **Minimum Node Version**: 16.0

---

*Built by Databricks Labs*