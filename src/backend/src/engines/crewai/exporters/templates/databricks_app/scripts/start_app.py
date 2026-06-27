#!/usr/bin/env python3
"""
Start script for the agent app.

The Kasal UI is a static SPA bundled in ./frontend. We build it once
(npm install && npm run build) and the agent server serves the resulting
frontend/dist directly via StaticFiles (see agent_server/start_server.py).
This is a SINGLE process — there is no separate frontend server and no MLflow
chat proxy (the proxy mishandled gzip and broke asset loading in the browser).

Requirements:
1. Build the frontend before the backend starts, so dist/ exists when the
   server mounts it.
2. Run only the backend process; exit as soon as it fails.
3. Print error logs if the backend fails.

Usage:
    start-app [OPTIONS]

All options are passed through to the backend server (start-server).
See 'uv run start-server --help' for available options.
"""

import argparse
import os
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

# Readiness patterns
BACKEND_READY = [r"Uvicorn running on", r"Application startup complete", r"Started server process"]


def check_port_available(port: int) -> bool:
    """Check if a port is available (nothing is actively listening on it)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect(("localhost", port))
        return False  # Something is listening
    except (ConnectionRefusedError, OSError):
        return True  # Nothing listening = available


class ProcessManager:
    def __init__(self, port=8000, no_ui=False):
        self.backend_process = None
        self.backend_ready = False
        self.failed = threading.Event()
        self.backend_log = None
        self.port = port
        self.no_ui = no_ui

    def check_ports(self):
        """Check that the (single) app port is available before starting."""
        backend_port = self.port
        if not check_port_available(backend_port):
            print("ERROR: Port already in use:\n")
            print(
                f"  Port {backend_port} (app) is already in use.\n"
                f"  To free it: lsof -ti :{backend_port} | xargs kill -9\n"
            )
            sys.exit(1)

    def monitor_process(self, process, name, log_file, patterns):
        is_ready = False
        try:
            for line in iter(process.stdout.readline, ""):
                if not line:
                    break

                line = line.rstrip()
                log_file.write(line + "\n")
                print(f"[{name}] {line}")

                # Check readiness
                if not is_ready and any(re.search(p, line, re.IGNORECASE) for p in patterns):
                    is_ready = True
                    self.backend_ready = True
                    print("✓ Backend is ready!")
                    print("\n" + "=" * 50)
                    if self.no_ui:
                        print("✓ Backend is ready! (running without UI)")
                        print(f"✓ API available at http://localhost:{self.port}")
                    else:
                        print("✓ App is ready!")
                        print(f"✓ Open the app at http://localhost:{self.port}")
                        print(f"✓ API available at http://localhost:{self.port}/invocations")
                    print("=" * 50 + "\n")

            process.wait()
            if process.returncode != 0:
                self.failed.set()

        except Exception as e:
            print(f"Error monitoring {name}: {e}")
            self.failed.set()

    def start_process(self, cmd, name, log_file, patterns, cwd=None):
        print(f"Starting {name}...")
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, cwd=cwd
        )

        thread = threading.Thread(
            target=self.monitor_process, args=(process, name, log_file, patterns), daemon=True
        )
        thread.start()
        return process

    def print_logs(self, log_path):
        print(f"\nLast 50 lines of {log_path}:")
        print("-" * 40)
        try:
            lines = Path(log_path).read_text().splitlines()
            print("\n".join(lines[-50:]))
        except FileNotFoundError:
            print(f"(no {log_path} found)")
        print("-" * 40)

    def cleanup(self):
        print("\n" + "=" * 42)
        print("Shutting down...")
        print("=" * 42)

        if self.backend_process:
            try:
                self.backend_process.terminate()
                self.backend_process.wait(timeout=5)
            except (subprocess.TimeoutExpired, Exception):
                self.backend_process.kill()

        if self.backend_log:
            self.backend_log.close()

    def build_frontend(self) -> bool:
        """Build the bundled SPA into frontend/dist so the server can serve it.

        Returns True if a build is present (or already up to date), False if the
        frontend is missing or the build failed.
        """
        frontend_dir = Path("frontend")
        if not frontend_dir.exists():
            print("WARNING: bundled frontend/ not found. Continuing with backend only.")
            return False

        for cmd, desc in [("npm install", "install"), ("npm run build", "build")]:
            print(f"Running npm {desc}...")
            result = subprocess.run(cmd.split(), cwd=frontend_dir, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"npm {desc} failed:\n{result.stdout}\n{result.stderr}")
                return False

        if not (frontend_dir / "dist" / "index.html").exists():
            print("WARNING: frontend build produced no dist/index.html. Backend only.")
            return False
        print("✓ Frontend built to frontend/dist")
        return True

    def run(self, backend_args=None):
        load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)
        if not os.environ.get("DATABRICKS_APP_NAME"):
            self.check_ports()

        # Build the SPA BEFORE the backend starts so frontend/dist exists when the
        # server mounts it. The backend then serves the UI and the API on one port.
        if not self.no_ui and not self.build_frontend():
            self.no_ui = True

        self.backend_log = open("backend.log", "w", buffering=1)

        try:
            backend_cmd = ["uv", "run", "start-server"]
            if backend_args:
                backend_cmd.extend(backend_args)

            self.backend_process = self.start_process(
                backend_cmd, "backend", self.backend_log, BACKEND_READY
            )
            print(f"\nMonitoring backend process (PID: {self.backend_process.pid})\n")

            # Wait for failure
            while not self.failed.is_set():
                time.sleep(0.1)
                if self.backend_process.poll() is not None:
                    self.failed.set()
                    break

            exit_code = self.backend_process.returncode if self.backend_process else 1
            print(f"\n{'=' * 42}\nERROR: backend process exited with code {exit_code}\n{'=' * 42}")
            self.print_logs("backend.log")
            return exit_code

        except KeyboardInterrupt:
            print("\nInterrupted")
            return 0

        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Start agent frontend and backend",
        usage="%(prog)s [OPTIONS]\n\nAll options are passed through to start-server. "
        "Use 'uv run start-server --help' for available options.",
    )
    parser.add_argument(
        "--no-ui",
        action="store_true",
        help="Run backend only, skip frontend UI",
    )
    args, backend_args = parser.parse_known_args()

    # Extract port from backend_args if specified
    port = 8000
    for i, arg in enumerate(backend_args):
        if arg == "--port" and i + 1 < len(backend_args):
            try:
                port = int(backend_args[i + 1])
            except ValueError:
                pass
            break

    sys.exit(ProcessManager(port=port, no_ui=args.no_ui).run(backend_args))


if __name__ == "__main__":
    main()
