"""Genie evaluation harness: ask a space its benchmark questions and grade the answers.

Modules are split by concern: ``questions`` (what to ask), ``numeric`` and ``tables``
(figures and result sets), ``grading`` (expected vs answer, verdicts), ``answer``
(the common answer record), ``conversation`` and ``fabric`` (the two agents),
``retry`` (rate-limit backoff), ``judge`` (LLM grading through Kasal's LLM layer),
``runner`` (the loop), ``report`` (summary and value redaction) and
``results_table`` (optional Delta output). The tool module wires real I/O in.
"""
