#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import logging
import time
import argparse
from pathlib import Path
from datetime import datetime

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Build the Kasal application")
parser.add_argument("--api-url", 
                    help="API URL to use in the frontend build (e.g. https://kasal-xxx.aws.databricksapps.com/api/v1)", 
                    required=True)
args = parser.parse_args()

# Configure logging
log_dir = Path("build/logs")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = log_dir / f"build_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("build")

class Builder:
    def __init__(self, api_url):
        self.root_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.dist_dir = self.root_dir / "dist"
        self.build_dir = self.root_dir / "build"
        self.temp_dir = self.build_dir / "temp"
        self.package_dir = self.build_dir / "package"
        self.frontend_dir = self.root_dir / "frontend"
        self.backend_dir = self.root_dir / "backend"
        self.docs_dir = self.root_dir / "docs"
        self.package_name = "kasal"
        self.version = "0.1.0"  # Could be dynamically determined
        self.api_url = api_url
        
        # Create build directories
        for d in [self.dist_dir, self.build_dir, self.temp_dir, self.package_dir]:
            d.mkdir(parents=True, exist_ok=True)
    
    def clean(self):
        """Clean previous build artifacts"""
        logger.info("Cleaning previous build artifacts")
        for d in [self.temp_dir, self.package_dir]:
            if d.exists():
                shutil.rmtree(d)
                d.mkdir(parents=True)
    
    def build_frontend(self):
        """Build the frontend React application"""
        logger.info("Building frontend")
        try:
            os.chdir(self.frontend_dir)
            subprocess.run(["npm", "install"], check=True)
            
            # Set the API URL environment variable for the build
            env = os.environ.copy()
            env["REACT_APP_API_URL"] = self.api_url
            logger.info(f"Setting REACT_APP_API_URL={self.api_url}")
            
            # Run the build with the environment variable
            subprocess.run(["npm", "run", "build"], check=True, env=env)
            
            # Create the frontend destination directory
            frontend_dest = self.package_dir / self.package_name / "frontend" / "static"
            frontend_dest.mkdir(parents=True, exist_ok=True)
            
            # Source is the built frontend
            frontend_build = self.frontend_dir / "build"
            logger.info(f"Copying frontend build from {frontend_build} to {frontend_dest}")
            
            # Ensure the build directory exists
            if not frontend_build.exists():
                logger.error(f"Frontend build directory not found: {frontend_build}")
                return False
            
            # Log the contents of the build directory
            logger.info(f"Frontend build directory contents:")
            for item in frontend_build.iterdir():
                logger.info(f"  {item.name}")
            
            # Copy everything from the build directory to the frontend/static directory
            for item in frontend_build.iterdir():
                if item.is_dir():
                    logger.info(f"Copying directory: {item.name}")
                    shutil.copytree(item, frontend_dest / item.name, dirs_exist_ok=True)
                else:
                    logger.info(f"Copying file: {item.name}")
                    shutil.copy2(item, frontend_dest)
            
            # Verify the contents after copying
            logger.info(f"Frontend static directory contents after copy:")
            for item in frontend_dest.iterdir():
                logger.info(f"  {item.name}")
            
            logger.info("Frontend build completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Frontend build failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Error in build_frontend: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            os.chdir(self.root_dir)
    
    def build_backend(self):
        """Prepare the backend Python package"""
        logger.info("Building backend")
        try:
            # Create the Python package structure
            backend_dest = self.package_dir / self.package_name / "backend"
            backend_dest.mkdir(parents=True, exist_ok=True)
            
            # Copy backend source code
            backend_src = self.backend_dir / "src"
            if backend_src.exists():
                shutil.copytree(
                    backend_src, 
                    backend_dest / "src",
                    dirs_exist_ok=True
                )
                
                # Create __init__.py files in all subdirectories of src to make imports work
                self._create_init_files(backend_dest / "src")
            
            # Copy migrations
            migrations_src = self.backend_dir / "migrations"
            if migrations_src.exists():
                shutil.copytree(
                    migrations_src, 
                    backend_dest / "migrations",
                    dirs_exist_ok=True
                )
            
            # Copy alembic.ini if it exists
            alembic_ini = self.backend_dir / "alembic.ini"
            if alembic_ini.exists():
                shutil.copy(alembic_ini, backend_dest)
            
            # Copy other necessary files
            for file in ["pyproject.toml", ".env.example"]:
                src_file = self.backend_dir / file
                if src_file.exists():
                    shutil.copy(src_file, backend_dest)
            
            logger.info("Backend build completed successfully")
            return True
        except Exception as e:
            logger.error(f"Backend build failed: {e}")
            return False
    
    def build_documentation(self):
        """Build documentation using MkDocs"""
        logger.info("Building documentation")
        try:
            # Check if mkdocs is installed
            try:
                subprocess.run(["mkdocs", "--version"], check=True, capture_output=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.info("Installing MkDocs and required extensions")
                subprocess.run([sys.executable, "-m", "pip", "install", "mkdocs", "mkdocs-material", "mkdocstrings[python]", "mkdocs-git-revision-date-localized-plugin"], check=True)
            
            # Build the documentation
            os.chdir(self.root_dir)
            result = subprocess.run(["mkdocs", "build", "--clean"], check=True, capture_output=True, text=True)
            
            # Log the output for debugging
            logger.info(f"MkDocs build output: {result.stdout}")
            if result.stderr:
                logger.warning(f"MkDocs build stderr: {result.stderr}")
            
            # Create the documentation destination directory
            docs_dest = self.package_dir / self.package_name / "docs"
            docs_dest.mkdir(parents=True, exist_ok=True)
            
            # Copy the built documentation
            site_dir = self.root_dir / "site"
            if site_dir.exists():
                logger.info(f"Copying documentation from {site_dir} to {docs_dest}")
                
                # List the contents of the site directory
                logger.info(f"Site directory contents before copying:")
                for item in site_dir.iterdir():
                    logger.info(f"  {item.name}")
                
                # Copy the site directory
                shutil.copytree(site_dir, docs_dest / "site", dirs_exist_ok=True)
                
                # Also copy the raw markdown files for reference
                logger.info(f"Copying raw documentation files from {self.docs_dir} to {docs_dest}")
                shutil.copytree(self.docs_dir, docs_dest / "markdown", dirs_exist_ok=True)
                
                # Create a simple index.html in the docs directory to redirect to the site subdirectory
                with open(docs_dest / "index.html", "w") as f:
                    f.write(f'''
<!DOCTYPE html>
<html>
<head>
    <title>{self.package_name.capitalize()} Documentation</title>
    <meta http-equiv="refresh" content="0;url=./site/index.html">
</head>
<body>
    <p>Redirecting to documentation...</p>
</body>
</html>
''')
                
                # List the contents of the docs directory
                logger.info(f"Docs directory contents after copying:")
                for item in docs_dest.iterdir():
                    logger.info(f"  {item.name}")
                    
                # Also log the site subdirectory contents
                site_subdir = docs_dest / "site"
                if site_subdir.exists():
                    logger.info(f"Site subdirectory contents:")
                    for item in site_subdir.iterdir():
                        logger.info(f"  {item.name}")
            else:
                logger.warning(f"Documentation site directory not found at {site_dir}. MkDocs build may have failed.")
            
            logger.info("Documentation build completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Documentation build failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Error in build_documentation: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            os.chdir(self.root_dir)
    
    def _create_init_files(self, directory):
        """Create __init__.py files recursively in all subdirectories"""
        for path in directory.glob('**'):
            if path.is_dir() and not (path / "__init__.py").exists():
                init_file = path / "__init__.py"
                init_file.touch()
                logger.info(f"Created __init__.py in {path}")
    
    def copy_entrypoint(self):
        """Copy the entrypoint file to the package"""
        logger.info("Copying entrypoint file")
        try:
            # Copy entrypoint.py to the package root module
            entrypoint = self.root_dir / "entrypoint.py"
            if entrypoint.exists():
                # Copy the content of entrypoint.py to __init__.py
                package_init = self.package_dir / self.package_name / "__init__.py"
                package_main = self.package_dir / self.package_name / "__main__.py"
                
                with open(entrypoint, "r") as src, open(package_init, "w") as dest:
                    # Extract functions and import them in __init__.py
                    content = src.read()
                    dest.write('"""\nKasal package.\n\nThis package contains both the frontend and backend components.\n"""\n\n')
                    dest.write('from kasal.entrypoint import run_app\n\n')
                    dest.write('__all__ = ["run_app"]\n')
                
                # Create a copy for __main__.py
                shutil.copy(entrypoint, package_main)
                
                # Also copy it as a standalone module for direct imports
                shutil.copy(entrypoint, self.package_dir / self.package_name / "entrypoint.py")
                
                logger.info("Entrypoint file copied successfully")
                return True
            else:
                logger.error("Entrypoint file not found")
                return False
        except Exception as e:
            logger.error(f"Error copying entrypoint file: {e}")
            return False
    
    def create_package_files(self):
        """Create package files including setup.py and __init__.py"""
        logger.info("Creating package files")
        
        # Create __init__.py files
        init_files = [
            self.package_dir / self.package_name / "backend" / "__init__.py",
            self.package_dir / self.package_name / "frontend" / "__init__.py",
            self.package_dir / self.package_name / "docs" / "__init__.py",
        ]
        
        for init_file in init_files:
            init_file.parent.mkdir(parents=True, exist_ok=True)
            init_file.touch()
        
        # Create setup.py
        setup_py = self.package_dir / "setup.py"
        with open(setup_py, "w") as f:
            f.write(f"""
from setuptools import setup, find_packages
import os
import glob

# Find all frontend static files recursively
frontend_static_files = []
frontend_dir = os.path.join("{self.package_name}", "frontend", "static")
for root, dirs, files in os.walk(frontend_dir):
    for file in files:
        file_path = os.path.join(root, file)
        frontend_static_files.append(file_path)

setup(
    name="{self.package_name}",
    version="{self.version}",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "fastapi>=0.110.0",
        "uvicorn[standard]>=0.27.0",
        "sqlalchemy>=2.0.27",
        "pydantic>=2.6.1",
        "pydantic-settings>=2.1.0",
        "alembic>=1.13.1",
        "asyncpg>=0.29.0",
        "httpx>=0.26.0",
        "python-jose[cryptography]>=3.3.0",
        "passlib[bcrypt]>=1.7.4",
        "python-multipart>=0.0.9",
        "tenacity>=8.2.3",
        "greenlet>=3.0.3",
        "mcp>=1.9.0",
        "mkdocs>=1.5.0",
        "mkdocs-material>=9.4.0",
        "mkdocstrings[python]>=0.24.0",
        "mkdocs-git-revision-date-localized-plugin>=1.2.0",
    ],
    python_requires=">=3.9",
    entry_points={{
        "console_scripts": [
            "{self.package_name}=kasal:run_app",
            "{self.package_name}-docs={self.package_name}.docs.serve:run_docs_server",
        ],
    }},
    package_data={{
        "{self.package_name}": [
            "frontend/static/**/*",
            "frontend/static/*",
            "backend/migrations/**/*",
            "backend/src/**/*",
            "backend/alembic.ini",
            "docs/site/**/*",
            "docs/markdown/**/*",
        ] + frontend_static_files,
    }},
    data_files=[
        ('', ['README.md']),
    ],
    zip_safe=False,
    project_urls={{
        'Documentation': 'https://your-domain.com/docs',
        'Source': 'https://github.com/yourusername/kasal',
    }},
)
""")
        
        # Create MANIFEST.in to include all necessary files
        manifest_in = self.package_dir / "MANIFEST.in"
        with open(manifest_in, "w") as f:
            f.write(f"""
recursive-include {self.package_name}/frontend/static *
recursive-include {self.package_name}/backend/migrations *
recursive-include {self.package_name}/backend/src *
recursive-include {self.package_name}/docs/site *
recursive-include {self.package_name}/docs/markdown *
include {self.package_name}/backend/alembic.ini
include README.md
""")
        
        # Copy README
        if (self.root_dir / "README.md").exists():
            shutil.copy(self.root_dir / "README.md", self.package_dir / "README.md")
        
        # Create docs server module
        docs_server_dir = self.package_dir / self.package_name / "docs"
        docs_server_dir.mkdir(parents=True, exist_ok=True)
        
        with open(docs_server_dir / "serve.py", "w") as f:
            f.write('''
import os
import sys
import webbrowser
import subprocess
import time

def run_docs_server():
    """Start the main application and open the browser to the documentation page"""
    print("Starting Kasal with documentation...")
    print("Opening documentation at http://localhost:8000/docs")
    
    try:
        # Try to import the main module and run it
        from kasal import run_app
        
        # Open the documentation in a new browser tab after a short delay
        def open_browser():
            time.sleep(2)  # Give the server time to start
            webbrowser.open("http://localhost:8000/docs")
        
        # Start browser in a separate thread so it doesn't block
        import threading
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
        
        # Start the app - this will block until the server is stopped
        run_app()
    except ImportError as e:
        print(f"Error: Could not import the Kasal application. Is it installed? Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_docs_server()
''')
        
        return True
    
    def build_wheel(self):
        """Build the wheel package"""
        logger.info("Building wheel package")
        try:
            os.chdir(self.package_dir)
            subprocess.run([sys.executable, "setup.py", "bdist_wheel"], check=True)
            
            # Copy the wheel to the dist directory
            wheel_files = list(Path("dist").glob("*.whl"))
            if wheel_files:
                wheel_file = wheel_files[0]
                shutil.copy(wheel_file, self.dist_dir)
                logger.info(f"Wheel package built successfully: {wheel_file.name}")
                return True
            else:
                logger.error("No wheel file was created")
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Wheel build failed: {e}")
            return False
        finally:
            os.chdir(self.root_dir)
    
    def run(self):
        """Run the full build process"""
        start_time = time.time()
        logger.info(f"Starting build process for {self.package_name} v{self.version}")
        
        self.clean()
        
        if not self.build_frontend():
            logger.error("Frontend build failed, stopping build process")
            return False
        
        if not self.build_backend():
            logger.error("Backend build failed, stopping build process")
            return False
        
        if not self.build_documentation():
            logger.error("Documentation build failed, continuing anyway")
            # Continue with the build process even if documentation fails
        
        if not self.copy_entrypoint():
            logger.error("Copying entrypoint file failed, stopping build process")
            return False
        
        if not self.create_package_files():
            logger.error("Creating package files failed, stopping build process")
            return False
        
        if not self.build_wheel():
            logger.error("Wheel build failed, stopping build process")
            return False
        
        elapsed_time = time.time() - start_time
        logger.info(f"Build completed successfully in {elapsed_time:.2f} seconds")
        logger.info(f"Wheel package available in {self.dist_dir}")
        return True

if __name__ == "__main__":
    builder = Builder(args.api_url)
    success = builder.run()
    sys.exit(0 if success else 1) 