"""GEPA helper library, extracted from prompt_optimization_service.

The service file had 1,027 lines of module-level helpers before its class even
started. These are pure functions — grading, parsing, model selection, thread
bridging — and they are far easier to reason about and test on their own than
buried above a 3,400-line service.
"""
