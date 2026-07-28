"""
Cron-scheduled runs.

Owns the schedule rows and the loop that fires them; the run it starts goes
through ExecutionService like any other, so nothing about scheduling leaks into
the execution paths.
"""
