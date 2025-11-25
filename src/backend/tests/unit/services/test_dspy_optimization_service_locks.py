import pytest
from src.services.dspy_optimization_service import _get_group_lock, _get_trace_hydration_lock


def test_group_lock_same_instance_per_key():
    lock1 = _get_group_lock('g1')
    lock2 = _get_group_lock('g1')
    assert lock1 is lock2


def test_group_lock_different_keys():
    lock1 = _get_group_lock('g1')
    lock2 = _get_group_lock('g2')
    assert lock1 is not lock2


def test_trace_hydration_lock_singleton():
    l1 = _get_trace_hydration_lock()
    l2 = _get_trace_hydration_lock()
    assert l1 is l2

