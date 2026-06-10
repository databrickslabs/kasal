"""
Security regression tests for memory_table validation (P2 / finding M2).

memory_table is interpolated into raw Lakebase SQL throughout the storage
backend, so a crafted value could inject SQL / comment out the appended
group_id tenant filter. It must be a strict SQL identifier.
"""
import pytest

from src.schemas.memory_backend import LakebaseMemoryConfig
from src.engines.crewai.memory.lakebase_storage_backend import _validate_table_name


class TestMemoryTableSchemaValidation:
    def test_accepts_valid(self):
        assert LakebaseMemoryConfig(memory_table="crew_memory").memory_table == "crew_memory"

    @pytest.mark.parametrize(
        "bad",
        [
            "crew_memory WHERE 1=1 --",
            "crew_memory cm UNION SELECT id,content FROM other",
            "a; DROP TABLE crew_memory; --",
            "crew memory",
            "crew_memory)",
            "",
        ],
    )
    def test_rejects_injection(self, bad):
        with pytest.raises(Exception):
            LakebaseMemoryConfig(memory_table=bad)


class TestStorageBackendTableValidation:
    def test_accepts_valid(self):
        assert _validate_table_name("crew_memory") == "crew_memory"

    @pytest.mark.parametrize(
        "bad",
        ["crew_memory WHERE 1=1 --", "a; DROP TABLE x", "a UNION SELECT", "bad name", ""],
    )
    def test_rejects_injection(self, bad):
        with pytest.raises(ValueError):
            _validate_table_name(bad)
