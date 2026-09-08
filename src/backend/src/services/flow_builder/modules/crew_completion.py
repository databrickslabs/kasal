"""Remember actual crew completion order for approval resume points."""

from functools import wraps
from inspect import iscoroutinefunction

from src.services.flow_builder.modules.flow_conditions import state_snapshot

_ORDER_KEY = "kasal_completed_crews"


def track_completion(method, name):
    if not iscoroutinefunction(method):
        return method

    @wraps(method)
    async def completed(self, *args, **kwargs):
        output = await method(self, *args, **kwargs)
        order = list(state_snapshot(self.state).get(_ORDER_KEY) or [])
        if name not in order:
            order.append(name)
            self.state[_ORDER_KEY] = order
        return output

    completed._meth = completed
    return completed


def completed_sequence(flow, method, fallback):
    order = state_snapshot(getattr(flow, "state", {})).get(_ORDER_KEY) or []
    return order.index(method) + 1 if method in order else fallback
