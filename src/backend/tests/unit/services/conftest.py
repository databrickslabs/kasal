"""Optional external decisions are off unless a unit test opts in explicitly."""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def jev_disabled_by_default():
    # Service units do not read a developer's live workspace configuration.
    # Gateway tests override this boundary to exercise enabled and failed calls;
    # settings tests exercise the real service and API-key delegation separately.
    with patch(
        "src.services.decisions.credentials.decision_credential",
        new=AsyncMock(return_value=None),
    ):
        yield
