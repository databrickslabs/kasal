# deploy

Build frontend and deploy to Databricks Apps.

## Command

```bash
cd ~/workspace/kasal && source ~/workspace/venv/bin/activate && python src/build.py && python src/deploy.py
```

## Description

This command:
1. Builds frontend static assets (npm install, npm build)
2. Copies documentation to public/docs
3. Copies built assets to frontend_static/
4. Deploys to Databricks Apps platform

## Usage

Simply type `/deploy` in Claude Code to build and deploy.

## Options

For deployment with specific options:
- Custom app name: `python src/deploy.py --app-name my-app`
- Custom user: `python src/deploy.py --user-name user@example.com`

## Prerequisites

- Databricks CLI configured
- Valid Databricks authentication
- Frontend dependencies installed
