#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("build")

class Builder:
    def __init__(self):
        self.root_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self.frontend_dir = self.root_dir / "frontend"
        self.docs_dir = self.root_dir / "docs"
    
    def build_frontend(self):
        """Build the frontend React application and copy to frontend_static"""
        logger.info("Building frontend")
        try:
            os.chdir(self.frontend_dir)
            
            # Install dependencies
            logger.info("Installing npm dependencies...")
            subprocess.run(["npm", "install"], check=True)
            
            # Copy documentation markdown files to frontend public folder before building
            frontend_public_docs = self.frontend_dir / "public" / "docs"
            logger.info(f"Copying documentation to {frontend_public_docs}")
            
            # Create or clean the public/docs directory
            if frontend_public_docs.exists():
                shutil.rmtree(frontend_public_docs)
            frontend_public_docs.mkdir(parents=True, exist_ok=True)
            
            # Copy all markdown files from docs directory
            for item in self.docs_dir.iterdir():
                if item.is_file() and item.suffix == '.md':
                    logger.info(f"Copying documentation file: {item.name}")
                    shutil.copy2(item, frontend_public_docs)
            
            # Run the build
            logger.info("Building React application...")
            subprocess.run(["npm", "run", "build"], check=True)
            
            # Source is the built frontend
            frontend_build = self.frontend_dir / "build"
            logger.info(f"Frontend build completed at {frontend_build}")
            
            # Ensure the build directory exists
            if not frontend_build.exists():
                logger.error(f"Frontend build directory not found: {frontend_build}")
                return False
            
            # Copy to frontend_static directory
            frontend_static_dest = self.root_dir / "frontend_static"
            logger.info(f"Copying frontend build to {frontend_static_dest}")
            
            # Create or clean the frontend_static directory
            if frontend_static_dest.exists():
                shutil.rmtree(frontend_static_dest)
            frontend_static_dest.mkdir(parents=True, exist_ok=True)
            
            # Copy everything from the build directory to frontend_static
            for item in frontend_build.iterdir():
                if item.is_dir():
                    logger.info(f"Copying directory to frontend_static: {item.name}")
                    shutil.copytree(item, frontend_static_dest / item.name, dirs_exist_ok=True)
                else:
                    logger.info(f"Copying file to frontend_static: {item.name}")
                    shutil.copy2(item, frontend_static_dest)
            
            # Copy docs to frontend_static as well
            frontend_static_docs = frontend_static_dest / "docs"
            frontend_static_docs.mkdir(parents=True, exist_ok=True)
            for item in self.docs_dir.iterdir():
                if item.is_file() and item.suffix == '.md':
                    shutil.copy2(item, frontend_static_docs)
            
            logger.info("Frontend build completed successfully")
            logger.info(f"Static files available at: {frontend_static_dest}")
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
    
    def run(self):
        """Run the frontend build process"""
        logger.info("Starting frontend build process")
        
        if not self.build_frontend():
            logger.error("Frontend build failed")
            return False
        
        logger.info("Frontend build completed successfully")
        return True

if __name__ == "__main__":
    builder = Builder()
    success = builder.run()
    sys.exit(0 if success else 1)