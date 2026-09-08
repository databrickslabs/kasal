"""Compatibility imports; HTTP dependency providers live in dependencies.providers."""

from src.dependencies.providers import GroupContextDep as GroupContextDep
from src.dependencies.providers import LegacySessionDep as LegacySessionDep
from src.dependencies.providers import LocalSessionDep as LocalSessionDep
from src.dependencies.providers import SessionDep as SessionDep
from src.dependencies.providers import WriteSessionDep as WriteSessionDep
from src.dependencies.providers import get_db as get_db
from src.dependencies.providers import get_group_context as get_group_context
from src.dependencies.providers import get_local_db as get_local_db
from src.dependencies.providers import get_repository as get_repository
from src.dependencies.providers import get_service as get_service
from src.dependencies.providers import get_smart_db_session as get_smart_db_session
