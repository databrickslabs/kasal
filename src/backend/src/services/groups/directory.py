"""Read-only discovery of people who have not necessarily visited Kasal."""

import asyncio
import json
import logging

import httpx
from databricks.sdk.core import Config
from pydantic import ValidationError

from src.core.databricks_app import DatabricksAppInstallation
from src.core.exceptions import KasalError
from src.schemas.user import DirectoryPerson
from src.utils.databricks_auth import get_auth_context

logger = logging.getLogger(__name__)
EMAIL_FALLBACK = " You can still add a person by their exact sign-in email."


async def search_directory(
    search: str, user_token: str | None
) -> list[DirectoryPerson]:
    installation = DatabricksAppInstallation.from_env()
    auth_method = "app" if installation.hosted else "configured"

    async def lookup():
        if installation.hosted:
            # Workspace SCIM is not an Apps user-authorization scope. This
            # system-admin-only operation uses the app's installed identity,
            # independent of the selected team's PAT or forwarded user token.
            def app_headers():
                return Config(
                    host=installation.host,
                    auth_type="oauth-m2m",
                    http_timeout_seconds=10,
                    retry_timeout_seconds=10,
                ).authenticate()

            workspace_url = installation.host
            headers = await asyncio.to_thread(app_headers)
        else:
            auth = await get_auth_context(user_token=user_token)
            if auth is None:
                raise KasalError(
                    "Databricks credentials are not configured for directory search."
                    + EMAIL_FALLBACK,
                    status_code=503,
                )
            workspace_url = auth.workspace_url
            headers = auth.get_headers()
        # JSON quoting prevents input from becoming a SCIM filter expression.
        value = json.dumps(search)

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"{workspace_url}/api/2.0/preview/scim/v2/Users",
                headers=headers,
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
    except KasalError:
        raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        # Never log response bodies, headers or the URL (which contains the
        # search text). Status + auth method make remote failures diagnosable.
        logger.warning(
            "User directory HTTP error: status=%s auth=%s", status, auth_method
        )
        if status == 403:
            identity = (
                "this app's service principal"
                if installation.hosted
                else "the configured Databricks identity"
            )
            detail = (
                f"Databricks denied directory access (HTTP 403). A Databricks workspace "
                f"administrator must check permission for {identity} to read workspace users."
            )
        elif status == 401:
            detail = "Databricks rejected the directory credentials (HTTP 401). Check the app's Databricks authentication."
        elif status == 429:
            detail = "Databricks directory search is rate limited (HTTP 429). Please try again shortly."
        else:
            detail = f"Databricks directory search failed (HTTP {status}). Check the app logs and workspace endpoint."
        raise KasalError(detail + EMAIL_FALLBACK, status_code=503) from exc
    except (TimeoutError, httpx.TimeoutException) as exc:
        logger.warning("User directory lookup timed out: auth=%s", auth_method)
        raise KasalError(
            "Databricks directory search timed out. Please try again." + EMAIL_FALLBACK,
            status_code=503,
        ) from exc
    except httpx.RequestError as exc:
        logger.warning(
            "User directory connection failed: auth=%s type=%s",
            auth_method,
            type(exc).__name__,
        )
        raise KasalError(
            "Cannot reach the Databricks directory. Check the app's network access to its workspace."
            + EMAIL_FALLBACK,
            status_code=503,
        ) from exc
    except Exception as exc:
        logger.warning(
            "User directory lookup failed: auth=%s type=%s",
            auth_method,
            type(exc).__name__,
        )
        raise KasalError(
            "Databricks directory search could not initialize or read its response. Check the app's Databricks credentials and logs."
            + EMAIL_FALLBACK,
            status_code=503,
        ) from exc
