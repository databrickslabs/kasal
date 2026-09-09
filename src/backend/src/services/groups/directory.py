"""Read-only discovery of people who have not necessarily visited Kasal."""

import asyncio
import json
import logging
from uuid import uuid4

import httpx
from databricks.sdk.core import Config
from pydantic import ValidationError

from src.core.databricks_app import DatabricksAppInstallation
from src.core.exceptions import KasalError
from src.schemas.user import DirectoryPerson
from src.utils.databricks_auth import get_auth_context

logger = logging.getLogger(__name__)
DIAGNOSTIC_VERSION = "DIR-v3"
PAGE_SIZE = 20
MAX_PAGES = 5
EMAIL_FALLBACK = " You can still add a person by their exact sign-in email."


def _http_status(error: Exception) -> int | None:
    """Read status only, including HTTP errors wrapped by SDK configuration."""
    current = error
    for _ in range(8):
        if current is None:
            break
        response = getattr(current, "response", None)
        for source in (response, current):
            status = getattr(source, "status_code", None)
            if type(status) is int and 300 <= status <= 599:
                return status
        current = current.__cause__ or current.__context__
    return None


async def search_directory(
    search: str, user_token: str | None
) -> list[DirectoryPerson]:
    installation = DatabricksAppInstallation.from_env()
    auth_method = "app" if installation.hosted else "configured"
    diagnostic = f"{DIAGNOSTIC_VERSION}/{uuid4().hex[:12]}"
    stage = "authentication"
    response_status = None
    logger.info(
        "User directory search started: diagnostic=%s auth=%s", diagnostic, auth_method
    )

    async def lookup():
        nonlocal stage, response_status
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
                    "Databricks credentials are not configured for directory search.",
                    status_code=503,
                )
            workspace_url = auth.workspace_url
            headers = auth.get_headers()
        # JSON quoting prevents input from becoming a SCIM filter expression.
        value = json.dumps(search)

        results = []
        received = inactive = unusable = pages = 0
        seen_emails = set()
        start_index = 1
        truncated = False
        async with httpx.AsyncClient(timeout=10) as client:
            for _ in range(MAX_PAGES):
                stage = "directory_request"
                response_status = None
                response = await client.get(
                    f"{workspace_url}/api/2.0/preview/scim/v2/Users",
                    headers=headers,
                    params={
                        "filter": f"userName co {value} or displayName co {value}",
                        "count": PAGE_SIZE,
                        "startIndex": start_index,
                        "attributes": "id,userName,displayName,active",
                    },
                )
                response_status = response.status_code
                response.raise_for_status()
                stage = "directory_response"
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Expected a SCIM list response")
                resources = payload.get("Resources")
                total = payload.get("totalResults")
                # SCIM can omit Resources when no matches exist, but a different
                # response shape must not silently masquerade as an empty list.
                if resources is None and type(total) is int and total == 0:
                    resources = []
                if not isinstance(resources, list) or any(
                    not isinstance(person, dict) for person in resources
                ):
                    raise ValueError("Expected SCIM Resources")
                pages += 1
                received += len(resources)
                for person in resources:
                    if person.get("active") is False:
                        inactive += 1
                        continue
                    try:
                        entry = DirectoryPerson(
                            email=person.get("userName"),
                            display_name=person.get("displayName"),
                        )
                    except ValidationError:
                        # Names and alternate email aliases are not proof of a
                        # sign-in identity. Never infer one to grant permissions.
                        unusable += 1
                        continue
                    if entry.email.lower() not in seen_emails:
                        seen_emails.add(entry.email.lower())
                        results.append(entry)
                    if len(results) == PAGE_SIZE:
                        break
                if not resources or len(results) == PAGE_SIZE:
                    break
                start_index += len(resources)
                if type(total) is int and start_index > total:
                    break
                if total is None and len(resources) < PAGE_SIZE:
                    break
            else:
                truncated = True

        logger.info(
            "User directory search completed: diagnostic=%s status=%s auth=%s "
            "pages=%s received=%s returned=%s inactive=%s unusable=%s truncated=%s",
            diagnostic,
            response_status,
            auth_method,
            pages,
            received,
            len(results),
            inactive,
            unusable,
            truncated,
        )
        if not results and unusable:
            raise KasalError(
                f"Databricks returned {received} matching directory records, but "
                "none provided an active, usable sign-in email. The directory response "
                "may contain limited identity details. Ask your Databricks administrator "
                "to check whether the app can read users' sign-in emails.",
                status_code=503,
            )
        if not results and inactive:
            raise KasalError(
                f"The {received} matching directory records checked were inactive. "
                "Try the person's full sign-in email or check their workspace access."
                + (" The search reached its page limit." if truncated else ""),
                status_code=503,
            )
        return results

    try:
        return await asyncio.wait_for(lookup(), timeout=15)
    except Exception as exc:
        status = (
            response_status
            if isinstance(exc, KasalError)
            else (_http_status(exc) or response_status)
        )
        identity = (
            "this app's service principal"
            if installation.hosted
            else "the configured Databricks identity"
        )
        if isinstance(exc, KasalError):
            detail = exc.detail
        elif status is not None and status >= 300:
            if stage == "authentication":
                detail = (
                    f"Databricks authentication failed (HTTP {status}) before the directory was queried. "
                    "Check the app's Databricks credentials and authentication configuration."
                )
            elif status == 403:
                detail = (
                    "Databricks denied directory access (HTTP 403). A Databricks workspace "
                    f"administrator must check permission for {identity} to read workspace users."
                )
            elif status == 401:
                detail = "Databricks rejected the directory credentials (HTTP 401). Check the app's Databricks authentication."
            elif status == 429:
                detail = "Databricks directory search is rate limited (HTTP 429). Please try again shortly."
            else:
                detail = f"Databricks directory search failed (HTTP {status}). Check the app logs and workspace endpoint."
        elif isinstance(exc, (TimeoutError, httpx.TimeoutException)):
            detail = "Databricks directory search timed out. Please try again."
        elif isinstance(exc, httpx.RequestError):
            detail = "Cannot reach the Databricks directory. Check the app's network access to its workspace."
        else:
            detail = (
                "Databricks directory search could not initialize or read its response. "
                "Check the app's Databricks credentials and logs."
            )

        # Never log exception messages, response bodies, headers or request URLs:
        # these can include credentials or the person's search text. The same
        # reference in the dialog and log identifies one search and code version.
        status_label = str(status) if status is not None else "unavailable"
        logger.warning(
            "User directory lookup failed: diagnostic=%s stage=%s status=%s auth=%s type=%s",
            diagnostic,
            stage,
            status_label,
            auth_method,
            type(exc).__name__,
        )
        raise KasalError(
            detail
            + EMAIL_FALLBACK
            + f" Diagnostic: {diagnostic}; stage={stage}; HTTP={status_label}; auth={auth_method}.",
            status_code=503,
        ) from exc
