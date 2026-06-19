"""Tests for src.utils.memory_paths — deterministic local memory store paths."""
import os
from unittest.mock import patch

from src.utils.memory_paths import (
    local_memory_root,
    local_memory_store_dir,
    sanitize_dir_component,
)


class TestSanitizeDirComponent:
    def test_keeps_safe_chars(self):
        assert sanitize_dir_component("user_dev-localhost.1") == "user_dev-localhost.1"

    def test_replaces_unsafe_chars(self):
        assert sanitize_dir_component("a/b c:d") == "a_b_c_d"

    def test_coerces_non_str(self):
        assert sanitize_dir_component(123) == "123"


class TestLocalMemoryRoot:
    def test_uses_env_override_and_creates_it(self, tmp_path):
        target = tmp_path / "kasal_mem"
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": str(target)}):
            root = local_memory_root()
        assert root == target
        assert root.is_dir()  # created if missing

    def test_expands_user_in_override(self, tmp_path):
        # Path.expanduser() reads $HOME (not Path.home()), so set HOME directly.
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": "~/kasal_mem_x", "HOME": str(tmp_path)}):
            root = local_memory_root()
        assert root == tmp_path / "kasal_mem_x"

    def test_defaults_under_home_when_unset(self, tmp_path):
        env = {k: v for k, v in os.environ.items() if k != "KASAL_MEMORY_DIR"}
        with patch.dict(os.environ, env, clear=True), \
             patch("src.utils.memory_paths.Path.home", return_value=tmp_path):
            root = local_memory_root()
        assert root == tmp_path / ".kasal" / "memory"
        assert root.is_dir()


class TestLocalMemoryStoreDir:
    def test_one_store_per_group(self, tmp_path):
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": str(tmp_path)}):
            store = local_memory_store_dir("user_dev_localhost")
        assert store == tmp_path / "kasal_default_user_dev_localhost"

    def test_sanitizes_group_id(self, tmp_path):
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": str(tmp_path)}):
            store = local_memory_store_dir("a/b c")
        assert store.name == "kasal_default_a_b_c"

    def test_blank_group_defaults(self, tmp_path):
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": str(tmp_path)}):
            store = local_memory_store_dir("")
        assert store.name == "kasal_default_default"

    def test_no_session_in_dir_name(self, tmp_path):
        """Session scoping lives in the record scope path, never the directory."""
        with patch.dict(os.environ, {"KASAL_MEMORY_DIR": str(tmp_path)}):
            store = local_memory_store_dir("grp")
        assert "session" not in store.name
