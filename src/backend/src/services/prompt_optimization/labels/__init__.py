"""A crew's labels: what its deliverable should contain, for the judges that
compare against it (Correctness, ExpectationsGuidelines).

* ``store`` keeps the labels the user confirmed in the MLflow Prompt Registry,
  one tagged prompt per crew and workspace, like the judges.
* ``review`` tags the crew's evaluation traces with the crew AND the workspace,
  and reads back the human review notes on them (the suggested labels, and the
  requirements distilled for ExpectationsGuidelines).
* ``operations`` is the service mixin the router and the run start call.

Blocking parts (``store``, ``review``) run inside ``asyncio.to_thread`` within
``mlflow_session(backend)``.
"""
