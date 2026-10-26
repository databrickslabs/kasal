#!/usr/bin/env python3
"""
List Power BI converter templates created by users who are not system admins.

WHY: A saved converter configuration with ``is_template=true`` is visible to
EVERY tenant. Since the N2 fix only a system administrator can create, update or
delete one, but templates a workspace user created BEFORE that fix are still
templates, so their configuration (and anything in it) is shown to all tenants.

WHAT: Read-only. Prints each ``is_template=true`` row whose ``created_by_email``
is not the email of a current system administrator. Nothing is changed. Review
each row, then have a system administrator delete it or ask its creator to
re-save it as a private configuration. There is deliberately no data migration.

USAGE (from src/backend, with the app's environment):
    python -m scripts.maintenance.list_unreviewed_powerbi_templates
    python -m scripts.maintenance.list_unreviewed_powerbi_templates --json
"""

import argparse
import asyncio
import json
import sys


async def _list() -> list:
    from src.db.session import routed_scoped_session
    from src.services.powerbi.conversions import ConverterService

    async with routed_scoped_session() as session:
        configs = await ConverterService(session).list_templates_created_by_non_admins()
        return [
            {
                "id": c.id,
                "name": c.name,
                "created_by_email": c.created_by_email,
                "group_id": c.group_id,
                "source_format": c.source_format,
                "target_format": c.target_format,
                "use_count": c.use_count,
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in configs
        ]


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--json", action="store_true", help="print JSON")
    args = parser.parse_args(argv)

    rows = asyncio.run(_list())
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("No templates created by non-admins.")
        return 0
    print(f"{len(rows)} template(s) created by a user who is not a system admin:")
    for r in rows:
        print(
            f"  id={r['id']} name={r['name']!r} by={r['created_by_email']} "
            f"group={r['group_id']} {r['source_format']}->{r['target_format']} "
            f"uses={r['use_count']} created={r['created_at']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
