"""Application services use core errors; HTTP translation belongs at the API edge."""

import ast
from pathlib import Path


def test_services_do_not_import_http_exceptions():
    services = Path(__file__).resolve().parents[3] / "src" / "services"
    offenders = []
    for path in services.rglob("*.py"):
        relative = path.relative_to(services)
        if relative.parts[:2] == ("export", "templates"):
            # These files are shipped into independent apps with their own HTTP edge.
            continue
        tree = ast.parse(path.read_text())
        framework_aliases = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.split(".")[0] in {"fastapi", "starlette"}:
                    for name in node.names:
                        if name.name in {"HTTPException", "*"}:
                            offenders.append(f"{relative}:{node.lineno}")
                        framework_aliases[name.asname or name.name] = node.module
            elif isinstance(node, ast.Import):
                for name in node.names:
                    if name.name.split(".")[0] in {"fastapi", "starlette"}:
                        framework_aliases[name.asname or name.name.split(".")[0]] = (
                            name.name
                        )
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "HTTPException":
                root = node.value
                while isinstance(root, ast.Attribute):
                    root = root.value
                if isinstance(root, ast.Name) and root.id in framework_aliases:
                    offenders.append(f"{relative}:{node.lineno}")
    assert not offenders, "Use src.core.exceptions in services: " + ", ".join(offenders)
