"""Compatibility imports; HTTP dependency providers live in dependencies.providers."""

from src.dependencies.providers import (
    GroupContextDep as GroupContextDep,
    LegacySessionDep as LegacySessionDep,
    LocalSessionDep as LocalSessionDep,
    SessionDep as SessionDep,
    WriteSessionDep as WriteSessionDep,
    get_group_context as get_group_context,
    get_repository as get_repository,
    get_service as get_service,
    get_db as get_db,
    get_local_db as get_local_db,
    get_smart_db_session as get_smart_db_session,
)
