"""MLflow built-in judges (Safety, RelevanceToQuery, ...) for Optimize.

``catalog`` lists what can be selected, ``bridge`` routes the judges' model
calls through LLMManager, and ``runner`` scores a deliverable and folds the
verdicts into the judge score. Judges run on demand; nothing is registered.

Import the submodules directly: this package stays free of mlflow imports at
import time, because the optimization runners must set mlflow's environment
before mlflow is first imported.
"""
