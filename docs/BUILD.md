# Kasal Build System

This document explains how to use the build system to create a distributable wheel package that includes both the backend and frontend components.

## Overview

The build script (`build.py`) packages the entire application into a Python wheel distribution that can be easily installed using pip. The wheel includes:

1. The backend Python code
2. The built frontend React static files
3. Configuration files and migrations

## Prerequisites

Before running the build script, ensure you have the following installed:

- Python 3.9+
- Node.js and npm
- wheel package (`pip install wheel`)

## Folder Structure

The build process will create the following folder structure:

```
kasal/
├── build/            # Build artifacts
│   ├── logs/         # Build logs
│   ├── package/      # Temporary package files
│   └── temp/         # Temporary files during build
└── dist/             # Final wheel package
```

## Building the Project

To build the project, run:

```bash
python build.py
```

This will:

1. Clean any previous build artifacts
2. Build the frontend React application
3. Package the backend Python code
4. Create the necessary package files
5. Build the wheel package
6. Copy the wheel to the `dist` directory

The build process generates detailed logs in the `build/logs` directory.

## Installing the Wheel

After building, you can install the wheel package using pip:

```bash
pip install dist/kasal-0.1.0-py3-none-any.whl
```

## Running the Application

Once installed, you can run the application using:

```bash
# Run directly as a module
python -m kasal.backend.src.main

# Or use the installed entry point
kasal
```

## Development Workflow

When developing:

1. Make changes to the frontend or backend code
2. Run `python build.py` to create a new wheel
3. Install the updated wheel
4. Test the application

## Troubleshooting

If you encounter issues during the build process:

1. Check the build logs in `build/logs/`
2. Ensure all prerequisites are installed
3. Verify that both frontend and backend can be built independently

For more detailed information, look at the error messages in the build logs. 