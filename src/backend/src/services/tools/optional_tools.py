"""The tool classes ``ToolFactory`` can build, each imported OPTIONALLY.

A tool whose module fails to import (a missing optional dependency, a broken
module) is logged and left as ``None`` so every other tool still works; the
factory skips ``None`` entries. Each name is declared first with its real class
type so the ``None`` fallback type-checks. ``tool_factory`` re-exports these
names (tests patch them there).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .agentbricks_tool import AgentBricksTool as _AgentBricksTool
    from .databricks_dashboard_creator_tool import (
        DatabricksDashboardCreatorTool as _DatabricksDashboardCreatorTool,
    )
    from .databricks_jobs_tool import DatabricksJobsTool as _DatabricksJobsTool
    from .databricks_knowledge_search_tool import (
        DatabricksKnowledgeSearchTool as _DatabricksKnowledgeSearchTool,
    )
    from .genie_space_generator_tool import (
        GenieSpaceGeneratorTool as _GenieSpaceGeneratorTool,
    )
    from .genie_tool import GenieTool as _GenieTool
    from .gmail_tool import GmailTool as _GmailTool
    from .mcp_adapter import MCPTool as _MCPTool
    from .measure_conversion_pipeline_tool import (
        MeasureConversionPipelineTool as _MeasureConversionPipelineTool,
    )
    from .metric_view_deployer_tool import (
        MetricViewDeployerTool as _MetricViewDeployerTool,
    )
    from .metric_view_validator_tool import (
        MetricViewValidatorTool as _MetricViewValidatorTool,
    )
    from .pbi_visual_ucmv_mapper_tool import (
        PBIVisualUCMVMapperTool as _PBIVisualUCMVMapperTool,
    )
    from .perplexity_tool import PerplexitySearchTool as _PerplexitySearchTool
    from .pipeline_config_generator_tool import (
        PipelineConfigGeneratorTool as _PipelineConfigGeneratorTool,
    )
    from .powerbi_connector_tool import PowerBIConnectorTool as _PowerBIConnectorTool
    from .powerbi_dax_executor_tool import (
        PowerBIDaxExecutorTool as _PowerBIDaxExecutorTool,
    )
    from .powerbi_field_parameters_calculation_groups_tool import (
        PowerBIFieldParametersCalculationGroupsTool as _PowerBIFieldParametersCalculationGroupsTool,
    )
    from .powerbi_hierarchies_tool import (
        PowerBIHierarchiesTool as _PowerBIHierarchiesTool,
    )
    from .powerbi_report_references_tool import (
        PowerBIReportReferencesTool as _PowerBIReportReferencesTool,
    )
    from .uc_metric_view_generator_tool import (
        UCMetricViewGeneratorTool as _UCMetricViewGeneratorTool,
    )
    from .ucmv_drift_monitor_tool import UCMVDriftMonitorTool as _UCMVDriftMonitorTool
    from .ucmv_genie_config_generator_tool import (
        UCMVGenieConfigGeneratorTool as _UCMVGenieConfigGeneratorTool,
    )


PerplexitySearchTool: Optional[type[_PerplexitySearchTool]]
try:
    from .perplexity_tool import PerplexitySearchTool
except ImportError:
    PerplexitySearchTool = None
    logging.warning("Could not import PerplexitySearchTool")

GenieTool: Optional[type[_GenieTool]]
try:
    from .genie_tool import GenieTool
except ImportError:
    GenieTool = None
    logging.warning("Could not import GenieTool")

AgentBricksTool: Optional[type[_AgentBricksTool]]
try:
    from .agentbricks_tool import AgentBricksTool
except ImportError:
    AgentBricksTool = None
    logging.warning("Could not import AgentBricksTool")

DatabricksJobsTool: Optional[type[_DatabricksJobsTool]]
try:
    from .databricks_jobs_tool import DatabricksJobsTool
except ImportError:
    DatabricksJobsTool = None
    logging.warning("Could not import DatabricksJobsTool")

DatabricksKnowledgeSearchTool: Optional[type[_DatabricksKnowledgeSearchTool]]
try:
    from .databricks_knowledge_search_tool import DatabricksKnowledgeSearchTool
except ImportError:
    DatabricksKnowledgeSearchTool = None
    logging.warning("Could not import DatabricksKnowledgeSearchTool")

GmailTool: Optional[type[_GmailTool]]
try:
    from .gmail_tool import GmailTool
except ImportError:
    GmailTool = None
    logging.warning("Could not import GmailTool")

# MCPTool - Import from mcp_adapter
MCPTool: Optional[type[_MCPTool]]
try:
    from .mcp_adapter import MCPTool
except ImportError:
    MCPTool = None
    logging.warning("Could not import MCPTool - MCP integration may not be available")

# Converter tools - Power BI connector and universal pipeline
MeasureConversionPipelineTool: Optional[type[_MeasureConversionPipelineTool]]
PowerBIConnectorTool: Optional[type[_PowerBIConnectorTool]]
try:
    from .measure_conversion_pipeline_tool import MeasureConversionPipelineTool
    from .powerbi_connector_tool import PowerBIConnectorTool
except ImportError as e:
    MeasureConversionPipelineTool = None
    PowerBIConnectorTool = None
    logging.warning("Could not import converter tools: %s", e)

# Power BI Hierarchies Tool
PowerBIHierarchiesTool: Optional[type[_PowerBIHierarchiesTool]]
try:
    from .powerbi_hierarchies_tool import PowerBIHierarchiesTool
except ImportError as e:
    PowerBIHierarchiesTool = None
    logging.warning("Could not import %s: %s", "PowerBIHierarchiesTool", e)

# Power BI Field Parameters & Calculation Groups Tool
PowerBIFieldParametersCalculationGroupsTool: Optional[
    type[_PowerBIFieldParametersCalculationGroupsTool]
]
try:
    from .powerbi_field_parameters_calculation_groups_tool import (
        PowerBIFieldParametersCalculationGroupsTool,
    )
except ImportError as e:
    PowerBIFieldParametersCalculationGroupsTool = None
    logging.warning(
        "Could not import %s: %s", "PowerBIFieldParametersCalculationGroupsTool", e
    )

# Power BI Report References Tool
PowerBIReportReferencesTool: Optional[type[_PowerBIReportReferencesTool]]
try:
    from .powerbi_report_references_tool import PowerBIReportReferencesTool
except ImportError as e:
    PowerBIReportReferencesTool = None
    logging.warning("Could not import %s: %s", "PowerBIReportReferencesTool", e)

# Power BI DAX Executor Tool
PowerBIDaxExecutorTool: Optional[type[_PowerBIDaxExecutorTool]]
try:
    from .powerbi_dax_executor_tool import PowerBIDaxExecutorTool
except Exception as e:  # noqa: BLE001 - any failure just drops this one tool
    PowerBIDaxExecutorTool = None
    logging.warning("Could not import %s: %s", "PowerBIDaxExecutorTool", e)

UCMetricViewGeneratorTool: Optional[type[_UCMetricViewGeneratorTool]]
try:
    from .uc_metric_view_generator_tool import UCMetricViewGeneratorTool
except ImportError as e:
    UCMetricViewGeneratorTool = None
    logging.warning("Could not import %s: %s", "UCMetricViewGeneratorTool", e)

MetricViewDeployerTool: Optional[type[_MetricViewDeployerTool]]
try:
    from .metric_view_deployer_tool import MetricViewDeployerTool
except ImportError as e:
    MetricViewDeployerTool = None
    logging.warning("Could not import %s: %s", "MetricViewDeployerTool", e)

MetricViewValidatorTool: Optional[type[_MetricViewValidatorTool]]
try:
    from .metric_view_validator_tool import MetricViewValidatorTool
except ImportError as e:
    MetricViewValidatorTool = None
    logging.warning("Could not import %s: %s", "MetricViewValidatorTool", e)

# Pipeline Config Generator Tool (API-direct, no LLM)
PipelineConfigGeneratorTool: Optional[type[_PipelineConfigGeneratorTool]]
try:
    from .pipeline_config_generator_tool import PipelineConfigGeneratorTool
except ImportError as e:
    PipelineConfigGeneratorTool = None
    logging.warning("Could not import %s: %s", "PipelineConfigGeneratorTool", e)

# Genie Space Generator Tool
GenieSpaceGeneratorTool: Optional[type[_GenieSpaceGeneratorTool]]
try:
    from .genie_space_generator_tool import GenieSpaceGeneratorTool
except ImportError as e:
    GenieSpaceGeneratorTool = None
    logging.warning("Could not import %s: %s", "GenieSpaceGeneratorTool", e)

# UCMV Genie Space Config Generator Tool
UCMVGenieConfigGeneratorTool: Optional[type[_UCMVGenieConfigGeneratorTool]]
try:
    from .ucmv_genie_config_generator_tool import UCMVGenieConfigGeneratorTool
except ImportError as e:
    UCMVGenieConfigGeneratorTool = None
    logging.warning("Could not import %s: %s", "UCMVGenieConfigGeneratorTool", e)

# PBI Visual-UCMV Mapper Tool (94)
PBIVisualUCMVMapperTool: Optional[type[_PBIVisualUCMVMapperTool]]
try:
    from .pbi_visual_ucmv_mapper_tool import PBIVisualUCMVMapperTool
except ImportError as e:
    PBIVisualUCMVMapperTool = None
    logging.warning("Could not import %s: %s", "PBIVisualUCMVMapperTool", e)

# Databricks Dashboard Creator Tool (95)
DatabricksDashboardCreatorTool: Optional[type[_DatabricksDashboardCreatorTool]]
try:
    from .databricks_dashboard_creator_tool import DatabricksDashboardCreatorTool
except ImportError as e:
    DatabricksDashboardCreatorTool = None
    logging.warning("Could not import %s: %s", "DatabricksDashboardCreatorTool", e)

# UCMV Drift Monitor Tool (keeps deployed metric views in sync with Power BI)
UCMVDriftMonitorTool: Optional[type[_UCMVDriftMonitorTool]]
try:
    from .ucmv_drift_monitor_tool import UCMVDriftMonitorTool
except ImportError as e:
    UCMVDriftMonitorTool = None
    logging.warning("Could not import %s: %s", "UCMVDriftMonitorTool", e)
