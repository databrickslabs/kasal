"""
Shipping a crew somewhere it can run without Kasal.

``crew`` deploys to a Databricks job/endpoint, ``app`` deploys the generated
Databricks App, ``crew_export`` builds the payload both of them ship.

The artefacts themselves — templates, the vendored A2UI renderer, the notebook
and python-project writers — are ``services/export/``. This package is the
delivery, that one is the packaging.
"""
