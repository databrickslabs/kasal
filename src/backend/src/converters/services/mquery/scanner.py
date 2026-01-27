"""
Power BI Admin API Scanner

This module handles scanning Power BI workspaces using the Admin API
to extract M-Query expressions, tables, relationships, and metadata.

Requires a Service Principal with Power BI Admin API permissions.

Author: Kasal Team
Date: 2025
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Tuple

import httpx

from .models import (
    SemanticModel,
    PowerBITable,
    ScanStatus,
    MQueryConversionConfig
)

logger = logging.getLogger(__name__)


class PowerBIAdminScanner:
    """
    Scanner for Power BI Admin API to extract semantic model metadata.

    This scanner uses the Admin API's getInfo endpoint which requires
    a Service Principal with admin permissions (Fabric Admin or Power BI Admin).

    Scanning flow:
    1. POST /admin/workspaces/getInfo - Initiate scan
    2. GET /admin/workspaces/scanStatus/{scanId} - Poll for completion
    3. GET /admin/workspaces/scanResult/{scanId} - Get results
    """

    API_BASE = "https://api.powerbi.com/v1.0/myorg"

    def __init__(
        self,
        access_token: str,
        config: Optional[MQueryConversionConfig] = None
    ):
        """
        Initialize the scanner.

        Args:
            access_token: OAuth token with Admin API permissions
            config: Optional configuration for scan options
        """
        self.access_token = access_token
        self.config = config or MQueryConversionConfig(
            tenant_id="",
            client_id="",
            client_secret="",
            workspace_id=""
        )
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """Async context manager entry"""
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            },
            timeout=180.0
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self._client:
            await self._client.aclose()

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _build_scan_url(self) -> str:
        """Build the scanning URL with query parameters"""
        params = []

        if self.config.include_lineage:
            params.append("lineage=True")
        if self.config.include_datasource_details:
            params.append("datasourceDetails=True")
        if self.config.include_dataset_schema:
            params.append("datasetSchema=True")
        if self.config.include_dataset_expressions:
            params.append("datasetExpressions=True")
        if self.config.include_artifact_users:
            params.append("getArtifactUsers=True")

        query_string = "&".join(params)
        return f"{self.API_BASE}/admin/workspaces/getInfo?{query_string}"

    async def initiate_scan(self, workspace_ids: List[str]) -> ScanStatus:
        """
        Initiate a workspace scan.

        Args:
            workspace_ids: List of workspace IDs to scan

        Returns:
            ScanStatus with scan_id for polling
        """
        url = self._build_scan_url()
        payload = {"workspaces": workspace_ids}

        logger.info(f"Initiating scan for {len(workspace_ids)} workspace(s)")
        logger.debug(f"Scan URL: {url}")

        if not self._client:
            async with httpx.AsyncClient(timeout=180.0) as client:
                response = await client.post(
                    url,
                    headers=self._get_headers(),
                    json=payload
                )
        else:
            response = await self._client.post(url, json=payload)

        response.raise_for_status()
        data = response.json()

        scan_id = data.get("id", "")
        logger.info(f"Scan initiated with ID: {scan_id}")

        return ScanStatus(
            scan_id=scan_id,
            status="Running",
            created_at=data.get("createdDateTime")
        )

    async def check_scan_status(self, scan_id: str) -> ScanStatus:
        """
        Check the status of an ongoing scan.

        Args:
            scan_id: ID of the scan to check

        Returns:
            ScanStatus with current status
        """
        url = f"{self.API_BASE}/admin/workspaces/scanStatus/{scan_id}"

        if not self._client:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url, headers=self._get_headers())
        else:
            response = await self._client.get(url)

        response.raise_for_status()
        data = response.json()

        return ScanStatus(
            scan_id=scan_id,
            status=data.get("status", "Unknown"),
            error=data.get("error")
        )

    async def get_scan_result(self, scan_id: str) -> Dict[str, Any]:
        """
        Get the results of a completed scan.

        Args:
            scan_id: ID of the completed scan

        Returns:
            Full scan result data
        """
        url = f"{self.API_BASE}/admin/workspaces/scanResult/{scan_id}"

        if not self._client:
            async with httpx.AsyncClient(timeout=180.0) as client:
                response = await client.get(url, headers=self._get_headers())
        else:
            response = await self._client.get(url)

        response.raise_for_status()
        return response.json()

    async def wait_for_scan(
        self,
        scan_id: str,
        timeout_seconds: int = 300,
        poll_interval: int = 5
    ) -> ScanStatus:
        """
        Wait for a scan to complete with polling.

        Args:
            scan_id: ID of the scan to wait for
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks

        Returns:
            Final ScanStatus

        Raises:
            TimeoutError: If scan doesn't complete within timeout
        """
        start_time = time.time()

        while True:
            status = await self.check_scan_status(scan_id)
            logger.debug(f"Scan status: {status.status}")

            if status.status == "Succeeded":
                logger.info(f"Scan {scan_id} completed successfully")
                return status

            if status.status == "Failed":
                logger.error(f"Scan {scan_id} failed: {status.error}")
                return status

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"Scan {scan_id} did not complete within {timeout_seconds} seconds"
                )

            logger.debug(f"Waiting {poll_interval}s before next status check...")
            await asyncio.sleep(poll_interval)

    async def scan_workspace(
        self,
        workspace_id: str,
        dataset_id: Optional[str] = None,
        timeout_seconds: int = 300
    ) -> Tuple[List[SemanticModel], Dict[str, Any]]:
        """
        Scan a workspace and extract semantic models.

        Args:
            workspace_id: Workspace ID to scan
            dataset_id: Optional specific dataset ID to filter
            timeout_seconds: Maximum time to wait for scan

        Returns:
            Tuple of (list of SemanticModels, raw scan data)
        """
        # Initiate scan
        scan_status = await self.initiate_scan([workspace_id])

        # Wait for completion
        final_status = await self.wait_for_scan(
            scan_status.scan_id,
            timeout_seconds=timeout_seconds
        )

        if final_status.status != "Succeeded":
            raise RuntimeError(f"Scan failed: {final_status.error}")

        # Get results
        raw_data = await self.get_scan_result(scan_status.scan_id)

        # Parse into SemanticModels
        semantic_models = []
        for workspace in raw_data.get("workspaces", []):
            ws_id = workspace.get("id")
            ws_name = workspace.get("name")

            for dataset in workspace.get("datasets", []):
                # Filter by dataset_id if specified
                if dataset_id and dataset.get("id") != dataset_id:
                    continue

                model = SemanticModel.from_scan_result(
                    dataset,
                    workspace_id=ws_id,
                    workspace_name=ws_name
                )
                semantic_models.append(model)

        logger.info(f"Extracted {len(semantic_models)} semantic model(s) from workspace")
        return semantic_models, raw_data

    async def scan_multiple_workspaces(
        self,
        workspace_ids: List[str],
        timeout_seconds: int = 600
    ) -> Tuple[List[SemanticModel], Dict[str, Any]]:
        """
        Scan multiple workspaces at once.

        Args:
            workspace_ids: List of workspace IDs to scan
            timeout_seconds: Maximum time to wait for scan

        Returns:
            Tuple of (list of SemanticModels, raw scan data)
        """
        # Initiate scan for all workspaces
        scan_status = await self.initiate_scan(workspace_ids)

        # Wait for completion
        final_status = await self.wait_for_scan(
            scan_status.scan_id,
            timeout_seconds=timeout_seconds,
            poll_interval=10  # Longer poll interval for multi-workspace scans
        )

        if final_status.status != "Succeeded":
            raise RuntimeError(f"Scan failed: {final_status.error}")

        # Get results
        raw_data = await self.get_scan_result(scan_status.scan_id)

        # Parse into SemanticModels
        semantic_models = []
        for workspace in raw_data.get("workspaces", []):
            ws_id = workspace.get("id")
            ws_name = workspace.get("name")

            for dataset in workspace.get("datasets", []):
                model = SemanticModel.from_scan_result(
                    dataset,
                    workspace_id=ws_id,
                    workspace_name=ws_name
                )
                semantic_models.append(model)

        logger.info(
            f"Extracted {len(semantic_models)} semantic model(s) "
            f"from {len(workspace_ids)} workspace(s)"
        )
        return semantic_models, raw_data

    def extract_tables_with_mquery(
        self,
        semantic_model: SemanticModel,
        include_hidden: bool = False
    ) -> List[PowerBITable]:
        """
        Extract tables that have M-Query source expressions.

        Args:
            semantic_model: The semantic model to extract from
            include_hidden: Whether to include hidden tables

        Returns:
            List of tables with M-Query expressions
        """
        tables = []

        for table in semantic_model.tables:
            # Skip hidden tables if not requested
            if table.is_hidden and not include_hidden:
                continue

            # Only include tables with source expressions
            if table.source_expressions:
                tables.append(table)

        logger.info(
            f"Found {len(tables)} table(s) with M-Query expressions "
            f"in model '{semantic_model.name}'"
        )
        return tables
