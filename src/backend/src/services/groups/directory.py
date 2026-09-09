"""Read-only discovery of people who have not necessarily visited Kasal."""

import asyncio
import json
import logging

import httpx
from pydantic import ValidationError

from src.core.exceptions import KasalError
from src.schemas.user import DirectoryPerson
from src.utils.databricks_auth import get_auth_context

logger = logging.getLogger(__name__)


async def search_directory(
    search: str, user_token: str | None
) -> list[DirectoryPerson]:
    async def lookup():
        auth = await get_auth_context(user_token=user_token)
        if auth is None:
            raise ValueError("Databricks authentication unavailable")
        # JSON quoting prevents input from becoming a SCIM filter expression.
        value = json.dumps(search)

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{auth.workspace_url}/api/2.0/preview/scim/v2/Users",
                headers=auth.get_headers(),
                params={
                    "filter": f"userName co {value} or displayName co {value}",
                    "count": 20,
                    "startIndex": 1,
                    "attributes": "id,userName,displayName,active",
                },
            )
            response.raise_for_status()
            results = []
            for person in response.json().get("Resources", [])[:20]:
                if person.get("active") is False:
                    continue
                try:
                    results.append(
                        DirectoryPerson(
                            email=person.get("userName"),
                            display_name=person.get("displayName"),
                        )
                    )
                except ValidationError:
                    # Only identities that can sign in by email can be provisioned.
                    continue
            return results

    try:
        return await asyncio.wait_for(lookup(), timeout=15)
    except Exception as exc:
        logger.warning("User directory lookup failed: %s", type(exc).__name__)
        raise KasalError(
            "Databricks directory is unavailable or access is denied. "
            "You can still add a person by their exact sign-in email.",
            status_code=503,
        ) from exc
