"""Customer, tenant and personal identifiers stay out of the tracked tree.

The repository is public. A review found one customer's name, catalogs and
schema prefixes, Power BI workspace / tenant / dataset / report GUIDs, a
workspace user id, a developer's personal catalog, named workspaces and short
secret prefixes in tracked files. They were replaced with placeholders
(``acme_*``, ``main``, ``00000000-0000-0000-0000-00000000000N``, ``example.*``);
this test keeps them from coming back.

It must not leak what it guards, so it holds salted SHA-256 fingerprints, never
the values, and a failure names the file and line, never the value (CI logs of
a public repository are public too).

How a file is checked: the text is lower-cased and split into tokens (runs of
letters and digits joined by ``_`` or ``-``). Every run of up to
``_MAX_PARTS`` consecutive parts of a token is joined with ``_`` and
fingerprinted, so ``x_y`` is caught inside ``a__x_y_z`` and as ``X-Y``. Short
values that could be part of an ordinary name live in ``_BANNED_WHOLE`` and
match only a whole token. File paths are checked the same way.

To guard another value, print its fingerprint locally and add it to
``_BANNED`` (or ``_BANNED_WHOLE``) with a comment naming its KIND only — never
commit the value. A value of more than ``_MAX_PARTS`` parts can never match;
guard a distinctive run of it instead::

    uv run python tests/unit/architecture/test_no_customer_identifiers.py <value>
"""

from __future__ import annotations

import hashlib
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[5]

_SALT = "kasal/identifier-guard/v1:"
_MAX_PARTS = 3
_TOKEN = re.compile(r"[a-z0-9]+(?:[_-][a-z0-9]+)*")
_SEP = re.compile(r"[_-]")
# Generated dependency metadata: long, machine-written, and never edited by hand.
_SKIP_SUFFIXES = ("package-lock.json", "uv.lock")

#: Salted fingerprints of removed identifiers, grouped by kind.
_BANNED = frozenset(
    {
        # customer name: abbreviations (2) and full-name forms (4)
        "9bdd4b9afdf17bbc171702b7",
        "4a081279a98242442a2fe9b2",
        "5522a78f70e5977741f85878",
        "00a4f22f57082e7d0532bade",
        "e9344ac5ea76b7f9de91be93",
        "5419bc318e11049362a1f089",
        # customer catalogs (5 variants, two catalogs)
        "817a60ff00ababe3294452e9",
        "d653f4cdf708bca3c8c90502",
        "e583a0dc3caa1e6f4052af31",
        "122d6fd01d1b746d6bd8c085",
        "1acca95d3d66acec30aba613",
        # customer schema prefixes
        "86cca71678a451e2001239c3",
        "2fad3c7b824e656f45e0d862",
        # personal catalog
        "a3e1919da56418d43001ff8b",
        # Power BI workspace, tenant, dataset, report and client GUIDs
        "5b2b09c54bea8fd45a8a56e7",
        "3d07bb58104b0a88b9f6f822",
        "675dc61daeb7204e8fb2011e",
        "77af84ef01e27b8b0b9b7bdb",
        "aef45d2cc5cddf1bffbb8e66",
        "4f8a77388891e2786a255d8f",
        "b625774110e84ec6c8cc2a46",
        "8e6026649d648a699ea4c74c",
        "40ba05b708b1687e7c622a34",
        # request id and user id captured from a live workspace
        "22ebde13584cd59727758bba",
        "b429f3ec07808f2028cdb1ed",
        # secret fragments
        "c4da6e00ed7d861ccaa20694",
        "b981bd8abe364ca70ace47e1",
        # Genie space ids
        "d2a5a0ecbd9f7d56aa1c1f05",
        "82551c419adf0c60e915c0b2",
        # named workspaces
        "449a380003eeb5d1bc1d794a",
        "38e226745dad2fe2a83a3bc0",
    }
)

#: Matched only when they are the WHOLE token, never a part of a longer name.
_BANNED_WHOLE = frozenset(
    {
        # customer short name (3 letters)
        "e3cea51ec0b9b7056fbad97a",
    }
)


def fingerprint(token: str) -> str:
    return hashlib.sha256((_SALT + token).encode()).hexdigest()[:24]


def _windows(token: str):
    parts = _SEP.split(token)
    for start in range(len(parts)):
        for end in range(start + 1, min(len(parts), start + _MAX_PARTS) + 1):
            yield "_".join(parts[start:end])


class _Matcher:
    """Caches the verdict per distinct token; most tokens repeat across files."""

    def __init__(
        self,
        banned: frozenset[str] = _BANNED,
        banned_whole: frozenset[str] = _BANNED_WHOLE,
    ) -> None:
        self._banned = banned
        self._banned_whole = banned_whole
        self._seen: dict[str, str | None] = {}

    def banned(self, token: str) -> str | None:
        if token not in self._seen:
            whole = fingerprint("_".join(_SEP.split(token)))
            self._seen[token] = (
                whole
                if whole in self._banned_whole
                else next(
                    (
                        fp
                        for w in _windows(token)
                        if (fp := fingerprint(w)) in self._banned
                    ),
                    None,
                )
            )
        return self._seen[token]

    def hits(self, text: str) -> list[tuple[int, str]]:
        """(line number, fingerprint) for every banned token in ``text``."""
        lowered = text.lower()
        if not any(self.banned(t) for t in set(_TOKEN.findall(lowered))):
            return []
        return [
            (lineno, fp)
            for lineno, line in enumerate(lowered.splitlines(), 1)
            for t in _TOKEN.findall(line)
            if (fp := self.banned(t))
        ]


def _tracked_files() -> list[str]:
    if shutil.which("git") is None:
        pytest.skip("git is not available")
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=False
    )
    if result.returncode != 0:
        pytest.skip("not a git checkout")
    return [p for p in result.stdout.decode().split("\0") if p]


def _read_text(path: pathlib.Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:  # deleted in the working tree, or a dangling symlink
        return None
    if b"\0" in data[:8192]:
        return None
    return data.decode("utf-8", errors="ignore")


def test_matcher_catches_spelling_and_embedding_variants():
    """Self-check on a synthetic value, so the real ones never appear here."""
    matcher = _Matcher(frozenset({fingerprint("zq_tenant"), fingerprint("x9y8")}))
    text = (
        "ok line\n"
        "FROM cat.zq_tenant_fm.t\n"  # prefix of a longer schema name
        "src: main.s.cat__ZQ-TENANT__fact\n"  # case, hyphen, flattened prefix
        "id = 'x9y8-...'\n"  # the leading part of a GUID
        "zqtenant zq tenant x9y80\n"  # different tokens: not a match
    )
    assert [n for n, _ in matcher.hits(text)] == [2, 3, 4]
    assert matcher.hits("docs/zq_tenant_example.json")


def test_whole_token_values_do_not_match_inside_longer_names():
    matcher = _Matcher(frozenset(), frozenset({fingerprint("qzx")}))
    text = (
        "Example QZX Ltd\n"  # whole token, any case
        "qzx_sales qzx-sales aqzxb\n"  # part of a longer token: not a match
        "sha512-abQZXcd==\n"
    )
    assert [n for n, _ in matcher.hits(text)] == [1]


def test_placeholders_are_not_banned():
    placeholders = "acme_prod.acme_datamart_fm main.test_schema example.com"
    assert _Matcher().hits(placeholders + " 00000000-0000-0000-0000-000000000001") == []


def test_no_removed_identifier_in_tracked_files():
    matcher = _Matcher()
    offenders: list[str] = []
    for rel in _tracked_files():
        if matcher.hits(rel):
            offenders.append(f"{rel} (path)")
        if rel.endswith(_SKIP_SUFFIXES):
            continue
        text = _read_text(REPO / rel)
        if text is None:
            continue
        offenders += [f"{rel}:{n} [{fp}]" for n, fp in matcher.hits(text)]
    assert not offenders, (
        "Removed customer/tenant/personal identifiers are back. Replace each with "
        "a placeholder (acme_*, main, 00000000-0000-0000-0000-00000000000N, "
        "example.com); the fingerprint says which kind:\n  " + "\n  ".join(offenders)
    )


if __name__ == "__main__":
    status = 0
    for value in sys.argv[1:]:
        parts = [p for p in _SEP.split(value.lower()) if p]
        if len(parts) > _MAX_PARTS:
            print(
                f"refused: {len(parts)} parts; the matcher only sees runs of "
                f"{_MAX_PARTS} or fewer, so fingerprint a distinctive run instead",
                file=sys.stderr,
            )
            status = 1
            continue
        print(f"{fingerprint('_'.join(parts))}  ({len(parts)} parts)")
    sys.exit(status)
