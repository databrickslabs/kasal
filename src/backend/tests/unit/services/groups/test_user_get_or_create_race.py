"""Two callers asking for the same user must not destroy each other's work.

Observed 11 seconds after a backend restart, when startup seeding and the first
authenticated request both resolved ``dev@localhost``:

    ERROR  UNIQUE constraint failed: users.email
    WARN   Race condition detected: User dev@localhost was created by another
           request. Fetching existing user.
    ERROR  UNIQUE constraint error but user dev@localhost still not found
           after race condition
    ERROR  greenlet_spawn has not been called; can't call await_only() here

The row demonstrably existed. ``get_by_email`` is a plain ``WHERE email = ?``
with no filters, so "still not found" is not a filtering problem — it is the
recovery destroying the winner.

Why retrying cannot fix it: the SQLite engine uses StaticPool, ONE shared
connection, so both callers are in the SAME transaction. B's INSERT conflicts
with A's *uncommitted* row, and B's rollback then discards A's INSERT as well.
Both callers fail and the row is gone. Hence serialising instead of retrying.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.groups import users as users_module
from src.services.groups.users import UserService


@pytest.fixture(autouse=True)
def _clear_locks():
    users_module._user_creation_locks.clear()
    yield
    users_module._user_creation_locks.clear()


class _SharedTransaction:
    """A stand-in for one SQLite connection shared by every caller.

    Models the property that matters: an INSERT is visible to the conflict check
    as soon as it is flushed, and a rollback throws away everything flushed so
    far — by anyone.
    """

    def __init__(self):
        self.rows: dict = {}
        self.uncommitted: dict = {}
        self.rollbacks = 0

    def select(self, email):
        # A caller sees its own uncommitted work and, on a SHARED connection,
        # everyone else's too — that is the whole point of StaticPool here.
        return self.rows.get(email) or self.uncommitted.get(email)

    def insert(self, email, user):
        if email in self.rows or email in self.uncommitted:
            raise Exception("UNIQUE constraint failed: users.email")
        self.uncommitted[email] = user

    def rollback(self):
        self.rollbacks += 1
        self.uncommitted.clear()  # discards OTHER callers' work too

    def commit(self):
        self.rows.update(self.uncommitted)
        self.uncommitted.clear()

    def count(self):
        """Rows that exist, committed or merely flushed. The request lifecycle
        commits at the end; within a request, flushed IS existing."""
        return len({**self.rows, **self.uncommitted})


def _service(txn, *, delay=0.0):
    """A UserService wired to the shared transaction above."""
    service = UserService(session=MagicMock())

    async def get_by_email(email):
        if delay:
            await asyncio.sleep(delay)  # widen the window deterministically
        return txn.select(email)

    async def create(data):
        # Yield before touching the transaction. A real flush is IO and gives
        # the loop a chance to run the other caller; without this the mock
        # executes atomically and the interleave under test never happens —
        # which made these tests pass against the BROKEN code.
        await asyncio.sleep(0)
        user = MagicMock(email=data["email"], is_system_admin=False)
        # FLUSH, not commit. base_repository.create flushes and explicitly does
        # NOT commit ("let the session dependency handle it"), which is the
        # condition that makes this unrecoverable: the row is visible to the
        # next INSERT's uniqueness check but is still rollback-able, so the
        # loser's rollback deletes the winner's row. A mock that committed here
        # would make the broken code look correct.
        txn.insert(data["email"], user)
        return user

    async def rollback():
        txn.rollback()

    service.user_repo.get_by_email = AsyncMock(side_effect=get_by_email)
    service.user_repo.get_by_username = AsyncMock(return_value=None)
    service.user_repo.create = AsyncMock(side_effect=create)
    service.session.rollback = AsyncMock(side_effect=rollback)
    service.session.expunge_all = MagicMock()
    service._handle_first_user_admin_setup = AsyncMock()
    return service


class TestConcurrentCallers:
    @pytest.mark.asyncio
    async def test_two_concurrent_callers_both_get_the_user(self):
        """The regression, stated as behaviour: nobody gets an exception and
        exactly one row is created."""
        txn = _SharedTransaction()
        a = _service(txn, delay=0.01)
        b = _service(txn, delay=0.01)

        results = await asyncio.gather(
            a.get_or_create_user_by_email("dev@localhost"),
            b.get_or_create_user_by_email("dev@localhost"),
            return_exceptions=True,
        )

        failures = [r for r in results if isinstance(r, BaseException)]
        assert not failures, f"a caller failed: {failures}"
        assert all(r is not None for r in results), "a caller got None"
        assert txn.count() == 1, "exactly one user row"

    @pytest.mark.asyncio
    async def test_the_row_survives(self):
        """The specific harm: the loser's rollback used to delete the winner's
        row, leaving "still not found" and no user at all."""
        txn = _SharedTransaction()
        await asyncio.gather(
            _service(txn, delay=0.01).get_or_create_user_by_email("dev@localhost"),
            _service(txn, delay=0.01).get_or_create_user_by_email("dev@localhost"),
        )
        assert txn.select("dev@localhost") is not None

    @pytest.mark.asyncio
    async def test_no_rollback_is_needed_at_all(self):
        """Serialising means the conflict never happens, so the recovery path —
        the thing that caused the damage — is never entered."""
        txn = _SharedTransaction()
        await asyncio.gather(
            *[
                _service(txn, delay=0.005).get_or_create_user_by_email("dev@localhost")
                for _ in range(5)
            ]
        )
        assert txn.rollbacks == 0
        assert txn.count() == 1

    @pytest.mark.asyncio
    async def test_the_second_caller_reads_rather_than_inserts(self):
        txn = _SharedTransaction()
        first, second = _service(txn), _service(txn)

        await first.get_or_create_user_by_email("dev@localhost")
        await second.get_or_create_user_by_email("dev@localhost")

        second.user_repo.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_different_emails_do_not_block_each_other(self):
        """The lock is per email; serialising all user creation would make a
        cold start with many users needlessly sequential."""
        txn = _SharedTransaction()
        await asyncio.gather(
            _service(txn, delay=0.01).get_or_create_user_by_email("a@example.com"),
            _service(txn, delay=0.01).get_or_create_user_by_email("b@example.com"),
        )
        assert txn.count() == 2
        assert set(users_module._user_creation_locks) == {
            "a@example.com",
            "b@example.com",
        }


class TestTheLockItself:
    @pytest.mark.asyncio
    async def test_one_lock_per_email(self):
        first = await users_module._lock_for_email("x@example.com")
        again = await users_module._lock_for_email("x@example.com")
        other = await users_module._lock_for_email("y@example.com")
        assert first is again, "two locks for one email would not serialise"
        assert first is not other

    @pytest.mark.asyncio
    async def test_concurrent_lock_creation_still_yields_one_lock(self):
        """Without the guard, two coroutines can each build a lock and then not
        share it — which looks like serialisation and is not."""
        locks = await asyncio.gather(
            *[users_module._lock_for_email("same@example.com") for _ in range(8)]
        )
        assert len({id(lock) for lock in locks}) == 1

    @pytest.mark.asyncio
    async def test_an_empty_email_does_not_create_a_lock(self):
        """It is rejected downstream; keying a lock on "" would collect one
        entry for every malformed request."""
        txn = _SharedTransaction()
        await _service(txn).get_or_create_user_by_email("")
        assert users_module._user_creation_locks == {}
