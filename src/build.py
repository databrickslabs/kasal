#!/usr/bin/env python3
import os
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
    
    def build_frontend(self):
        """Use the same npm build lifecycle as deployment and wheel packaging."""
        npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
        try:
            subprocess.run([npm_cmd, "run", "build"], cwd=self.root_dir, check=True)
            frontend_static = self.root_dir / "frontend_static"
            if not (frontend_static / "index.html").is_file():
                logger.error("Frontend build did not produce frontend_static/index.html")
                return False
            logger.info("Static files available at: %s", frontend_static)
            return True
        except (OSError, subprocess.CalledProcessError):
            logger.exception("Frontend build failed")
            return False

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