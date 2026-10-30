"""A JSON column whose credential-shaped values are encrypted in the database.

Tool configurations carry Power BI service-principal secrets, passwords, PATs
and API keys. With this type the ORM always holds plaintext, so services, the
crew and flow subprocesses and the tool factory need no decrypt step of their
own, while every statement that writes the column sends ciphertext:

- On write, each non-empty string under a key that
  ``sensitive_data_utils.encrypts_at_rest`` names is encrypted, in nested
  dictionaries and lists too. That rule is broader than ``is_sensitive_key``,
  which decides masking: a count such as ``max_tokens`` is shown in API
  responses but still encrypted here. A value that is already encrypted is
  left alone, so nothing is encrypted twice.
- On read, every ``ENC:`` value under those same keys is decrypted (so an
  ordinary field that happens to start ``ENC:`` is left alone, and a row read
  and saved again stays encrypted). A row written before this type existed
  reads unchanged. A value
  that cannot be decrypted (the key changed) comes back still encrypted: it is
  never blanked, and a later save writes it back as it was.

Only the Python side changes: the database type is still JSON, so applying it
to an existing column needs no DDL and no migration.
"""

from typing import Optional

from sqlalchemy import JSON
from sqlalchemy.engine import Dialect
from sqlalchemy.types import TypeDecorator

from src.utils.sensitive_data_utils import (
    decrypt_sensitive_json,
    encrypt_sensitive_json,
)


class SensitiveJSON(TypeDecorator[object]):
    """JSON with its sensitive leaves encrypted at rest."""

    impl = JSON
    cache_ok = True
    # TypeDecorator does not inherit these two from JSON; without them the
    # column would treat values differently from the plain JSON it replaces.
    # Dicts and lists are not hashable (a query de-duplicating rows would raise),
    # and an explicit None is stored as JSON null (JSON's none_as_null=False).
    hashable = False
    should_evaluate_none = True

    def process_bind_param(
        self, value: Optional[object], dialect: Dialect
    ) -> Optional[object]:
        if value is None:
            return None
        return encrypt_sensitive_json(value)

    def process_result_value(
        self, value: Optional[object], dialect: Dialect
    ) -> Optional[object]:
        if value is None:
            return None
        return decrypt_sensitive_json(value)
