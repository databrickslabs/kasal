"""Current values of a semantic model's Power Query parameters, from the Power BI REST API.

A native query often splices parameters into its SQL (``'" & RE_Version & "'``). Crew 1 only
guessed the parameter names from the M and left the values as TODO, so the source layer had to
assume '' for them, and a filter such as ``bic_chversion = ''`` then removed every row. The
dataset's ``/parameters`` endpoint returns each parameter's ``currentValue`` with the same
service principal; those values replace the TODO entries in ``parameter_defaults``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

import requests

from .http_retry import send_with_throttle_retry

logger = logging.getLogger(__name__)

_API = "https://api.powerbi.com/v1.0/myorg/groups/{workspace}/datasets/{dataset}/parameters"


def fetch_dataset_parameters(
    token: str, workspace_id: str, dataset_id: str
) -> dict[str, str]:
    """``{name: currentValue}``; ``{}`` when the call fails (the TODO guesses then stay)."""
    from .pipeline_config import _headers, _pbi_id

    try:
        url = _API.format(workspace=_pbi_id(workspace_id), dataset=_pbi_id(dataset_id))
        response = send_with_throttle_retry(
            lambda: requests.get(
                url, headers=_headers(token), timeout=60
            ),  # fixed Power BI host, ids validated — nosemgrep: tmp.databricks.ssrf.requests-user-url
            "dataset parameters",
        )
        if response.status_code != 200:
            logger.warning(
                "[PipelineConfigGen] dataset parameters HTTP %s", response.status_code
            )
            return {}
        rows = response.json().get("value") or []
    except (requests.RequestException, ValueError) as error:
        logger.warning("[PipelineConfigGen] dataset parameters unavailable: %s", error)
        return {}
    return {
        str(row["name"]): str(
            row.get("currentValue") if row.get("currentValue") is not None else ""
        )
        for row in rows
        if isinstance(row, dict) and row.get("name")
    }


def with_live_parameters(
    config: dict, token: str, workspace_id: str, dataset_id: str
) -> dict:
    """``config`` with ``parameter_defaults`` taken from the model where it has a value."""
    live = fetch_dataset_parameters(token, workspace_id, dataset_id)
    if live:
        guessed: Mapping[str, str] = config.get("parameter_defaults") or {}
        config["parameter_defaults"] = {**guessed, **live}
        logger.info(
            "[PipelineConfigGen] %d Power Query parameter value(s) from the model",
            len(live),
        )
    return config
