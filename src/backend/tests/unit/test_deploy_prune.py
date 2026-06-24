"""Tests for deploy.py — recursive remote prune.

Regression coverage for the deploy failure where the Databricks Apps source
sync overran the platform's hard 10-minute app-start window
(``App process did not start within 10 minutes``).

Root cause: ``prune_remote_dir`` was shallow (top-level only). Vite
content-hashes every frontend chunk (``index-<hash>.js``), so each build emits
NEW filenames and the old ones are orphaned. ``databricks sync`` only
adds/overwrites — it never mirror-deletes — so the orphans piled up inside
``frontend_static/assets`` across deploys (2400+ remote vs ~150 real) and got
re-synced into the app compute every time. The shallow prune skipped them
because ``assets/`` (a directory) still exists locally by name.

The fix makes the prune recurse into subdirectories that still exist locally,
so orphaned files *inside* them are deleted.
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))

from deploy import prune_remote_dir


def _entry(path):
    """A fake workspace ObjectInfo with just the .path attribute used by prune."""
    e = MagicMock()
    e.path = path
    return e


def _client(tree):
    """Build a fake WorkspaceClient whose .workspace.list returns `tree[dir]`.

    `tree` maps a remote dir path -> list of child remote paths.
    """
    client = MagicMock()

    def _list(remote_dir):
        return [_entry(p) for p in tree.get(remote_dir, [])]

    client.workspace.list.side_effect = _list
    return client


def _deleted_paths(client):
    return [c.kwargs.get("path", c.args[0]) for c in client.workspace.delete.call_args_list]


def test_prunes_orphaned_top_level_entry(tmp_path):
    """A remote file with no local counterpart is deleted."""
    (tmp_path / "keep.js").write_text("x")
    client = _client({"/remote": ["/remote/keep.js", "/remote/stale.js"]})

    prune_remote_dir(client, "/remote", tmp_path)

    deleted = _deleted_paths(client)
    assert deleted == ["/remote/stale.js"]


def test_recurses_into_subdir_to_prune_orphaned_hashed_chunks(tmp_path):
    """The core regression: orphaned files INSIDE a still-present subdir
    (frontend_static/assets) must be pruned, not skipped."""
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "index-NEWHASH.js").write_text("x")  # current build's chunk

    client = _client({
        "/remote": ["/remote/assets"],
        "/remote/assets": [
            "/remote/assets/index-NEWHASH.js",   # matches local — keep
            "/remote/assets/index-OLDHASH1.js",  # orphan from a prior build
            "/remote/assets/index-OLDHASH2.js",  # orphan from a prior build
        ],
    })

    prune_remote_dir(client, "/remote", tmp_path)

    deleted = sorted(_deleted_paths(client))
    assert deleted == [
        "/remote/assets/index-OLDHASH1.js",
        "/remote/assets/index-OLDHASH2.js",
    ]
    # The directory itself and the still-current chunk are left alone.
    assert "/remote/assets" not in deleted
    assert "/remote/assets/index-NEWHASH.js" not in deleted


def test_orphaned_subdir_deleted_wholesale_not_recursed(tmp_path):
    """A remote directory absent locally is deleted in one recursive call —
    we must NOT descend into it (no need, and it may be huge)."""
    # tmp_path is empty: nothing local matches the remote "oldfeature" dir.
    client = _client({
        "/remote": ["/remote/oldfeature"],
        "/remote/oldfeature": ["/remote/oldfeature/a.js"],
    })

    prune_remote_dir(client, "/remote", tmp_path)

    deleted = _deleted_paths(client)
    assert deleted == ["/remote/oldfeature"]
    # delete called with recursive=True; never listed the dir's contents.
    assert client.workspace.delete.call_args_list[0].kwargs.get("recursive") is True
    client.workspace.list.assert_called_once_with("/remote")


def test_keeps_everything_when_remote_matches_local(tmp_path):
    """No deletions when the remote tree mirrors the local bundle."""
    (tmp_path / "a.js").write_text("x")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.js").write_text("x")

    client = _client({
        "/remote": ["/remote/a.js", "/remote/sub"],
        "/remote/sub": ["/remote/sub/b.js"],
    })

    prune_remote_dir(client, "/remote", tmp_path)

    client.workspace.delete.assert_not_called()


def test_missing_remote_dir_is_a_noop(tmp_path):
    """If the remote dir doesn't exist yet, prune silently returns."""
    client = MagicMock()
    client.workspace.list.side_effect = Exception("RESOURCE_DOES_NOT_EXIST")

    prune_remote_dir(client, "/remote", tmp_path)

    client.workspace.delete.assert_not_called()


def test_delete_error_is_swallowed_and_does_not_abort(tmp_path):
    """A failed delete is logged but must not stop pruning the rest."""
    client = _client({"/remote": ["/remote/stale1.js", "/remote/stale2.js"]})
    client.workspace.delete.side_effect = [Exception("perm denied"), None]

    prune_remote_dir(client, "/remote", tmp_path)  # must not raise

    assert _deleted_paths(client) == ["/remote/stale1.js", "/remote/stale2.js"]
