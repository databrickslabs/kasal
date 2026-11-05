"""Pure helpers for the Genie Space Sync tool.

Layout and ID handling (``space_files``, ``ids``), the Genie REST calls behind an
injected HTTP callable (``genie_api``, ``deploy_plan``), the deployed-object
catalog (``metric_views``, ``sql_results``, ``sql_checks``), the checks
(``checks_space``, ``checks_mapping``, ``checks_report``) and ``verdicts``.
The tool module wires real Databricks I/O into these.
"""
