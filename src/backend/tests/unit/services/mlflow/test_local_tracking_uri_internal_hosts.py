"""A local MLflow server on another host is never an internal address.

Apps security scan (2026-10-28): ``experiment_id`` GETs the configured server
from the backend, and ``validate_local_tracking_uri`` accepted https to ANY
host — so a workspace member could aim that request at cloud metadata or a
private address inside the deployment's network.
"""

import pytest

from src.services.mlflow.local import local_tracking_uri, validate_local_tracking_uri


@pytest.mark.parametrize(
    "uri",
    [
        "https://169.254.169.254/latest/meta-data",
        "https://10.0.0.5:5000",
        "https://192.168.1.20",
        "https://metadata.google.internal",
        "https://mlflow.corp.internal",
    ],
)
def test_internal_destinations_are_refused(uri):
    with pytest.raises(ValueError, match="internal address"):
        validate_local_tracking_uri(uri)
    assert local_tracking_uri(uri) is None  # the read path refuses it too


@pytest.mark.parametrize(
    "uri",
    ["http://127.0.0.1:5555", "http://localhost:5000", "https://mlflow.example.com"],
)
def test_this_machine_and_public_https_still_work(uri):
    assert validate_local_tracking_uri(uri) == uri
