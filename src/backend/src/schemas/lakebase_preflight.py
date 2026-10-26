"""Schemas for the Lakebase preflight diagnostic report.

The preflight runs when the user connects to / enables Lakebase and reports
whether this app's service principal can operate the schema, plus actionable
remediation when it cannot. See services/databricks/lakebase/preflight.py.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class PreflightCheck(BaseModel):
    name: str
    ok: bool
    detail: str = ""


class PreflightTable(BaseModel):
    name: str
    exists: bool
    owner: Optional[str] = None
    owned_by_app: Optional[bool] = None
    missing_columns: List[str] = Field(default_factory=list)
    status: str  # ok | auto_fixable | action_required | absent


class PreflightRemediation(BaseModel):
    summary: str
    steps: List[str] = Field(default_factory=list)
    commands: List[str] = Field(default_factory=list)


class LakebasePreflightReport(BaseModel):
    """Result of the Lakebase connect preflight.

    status:
      - ``healthy``          — the app SP can operate every required table.
      - ``action_required``  — a table it does not own is missing columns and the
                               app cannot add them; ``remediation`` says how to fix.
      - ``error``            — the preflight itself could not run (connect/auth).
    """

    status: str
    current_user: Optional[str] = None
    checks: List[PreflightCheck] = Field(default_factory=list)
    tables: List[PreflightTable] = Field(default_factory=list)
    remediation: Optional[PreflightRemediation] = None
