"""Tests for the prompt-registry judge store.

Judges are versioned prompts (``kasal_judge__<full_name>``), on Unity Catalog
or a local server — never MLflow scheduled scorers (issue #7).
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.prompt_optimization import judge_registry as jr


class _Page(list):
    """A PagedList stand-in: a list with a continuation token."""

    def __init__(self, items, token=None):
        super().__init__(items)
        self.token = token


def _version(template, model="qwen-30b", version=3):
    return SimpleNamespace(
        template=template,
        tags={jr.TAG_MODEL: model, jr.TAG_KIND: "judge"},
        version=version,
    )


def _prompts(*names):
    return [SimpleNamespace(name=n) for n in names]


def _versions(*numbers, uc=False):
    """search_prompt_versions result: a list on OSS, a response proto carrying
    ``prompt_versions`` on Unity Catalog (versions arrive as strings there)."""
    items = [SimpleNamespace(version=(str(n) if uc else n)) for n in numbers]
    return SimpleNamespace(prompt_versions=items) if uc else items


def _client(versions=(3,), uc=False, template="criteria"):
    client = MagicMock()
    client.search_prompt_versions.return_value = _versions(*versions, uc=uc)
    client.load_prompt.side_effect = (
        lambda name, version=None, allow_missing=False: _version(
            f"{template} of {name}", version=int(version)
        )
    )
    return client


class TestNaming:
    def test_prompt_names_carry_the_prefix_and_the_uc_schema(self):
        assert jr.JudgeRegistry("http://x", client=MagicMock()).prompt_name("acc") == (
            "kasal_judge__acc"
        )
        uc = jr.JudgeRegistry("databricks-uc", "main.kasal", client=MagicMock())
        assert uc.prompt_name("crew_88ab4478823c__acc") == (
            "main.kasal.kasal_judge__crew_88ab4478823c__acc"
        )

    def test_full_name_of_reads_the_leaf_and_ignores_other_prompts(self):
        assert jr.JudgeRegistry.full_name_of("main.kasal.kasal_judge__acc") == "acc"
        assert (
            jr.JudgeRegistry.full_name_of("kasal_judge__crew_x__acc") == "crew_x__acc"
        )
        # GEPA's crew prompts share the schema.
        assert jr.JudgeRegistry.full_name_of("main.kasal.kasal_crew_88ab_grp") is None
        assert jr.JudgeRegistry.full_name_of("kasal_judge__") is None

    def test_uc_schema_of(self):
        assert (
            jr.uc_schema_of("databricks-uc", "main.kasal.kasal_judge_grp")
            == "main.kasal"
        )
        assert jr.uc_schema_of("http://127.0.0.1:5555", "kasal_judge_grp") is None

    def test_split_crew_name(self):
        assert jr.split_crew_name("crew_88ab4478823c__accuracy") == (
            "88ab4478823c",
            "accuracy",
        )
        assert jr.split_crew_name("accuracy") == (None, "accuracy")


class TestGuidelines:
    def test_round_trip_matches_mlflows_block_format(self):
        text = jr.with_guidelines(
            "Rate {{ outputs }}.", ["Be strict.", "Cite sources."]
        )
        assert text == (
            "Rate {{ outputs }}.\n\nDistilled Guidelines (2):\n"
            "  - Be strict.\n  - Cite sources.\n"
        )
        assert jr.strip_guidelines(text) == (
            "Rate {{ outputs }}.",
            ["Be strict.", "Cite sources."],
        )

    def test_plain_instructions_pass_through(self):
        assert jr.with_guidelines("Rate it.", []) == "Rate it."
        assert jr.strip_guidelines("Rate it.") == ("Rate it.", [])


class TestReads:
    def test_list_pages_filters_by_crew_and_skips_foreign_prompts(self):
        client = _client(versions=(1, 3, 2))
        client.search_prompts.side_effect = [
            _Page(_prompts("kasal_judge__lib", "kasal_crew_88ab_grp"), token="t2"),
            _Page(_prompts("kasal_judge__crew_88ab4478823c__lib")),
        ]
        registry = jr.JudgeRegistry("http://x", client=client)

        everything = registry.list()
        assert [s.full_name for s in everything] == ["lib", "crew_88ab4478823c__lib"]
        assert everything[1].crew_id == "88ab4478823c"
        assert everything[1].display_name == "lib"
        assert everything[1].name == "crew_88ab4478823c__lib"  # the identity
        assert everything[0].model == "qwen-30b" and everything[0].version == 3
        # Local registries filter by name prefix; both pages were walked.
        first_call = client.search_prompts.call_args_list[0].kwargs
        assert first_call["filter_string"] == "name LIKE 'kasal_judge__%'"
        assert client.search_prompts.call_args_list[1].kwargs["page_token"] == "t2"

        client.search_prompts.side_effect = [
            _Page(_prompts("kasal_judge__lib", "kasal_judge__crew_88ab4478823c__lib"))
        ]
        assert [s.full_name for s in registry.list("crew_88ab4478823c__")] == [
            "crew_88ab4478823c__lib"
        ]

    def test_uc_search_uses_catalog_and_schema(self):
        client = MagicMock()
        client.search_prompts.return_value = _Page([])
        jr.JudgeRegistry("databricks-uc", "main.kasal", client=client).list()
        assert client.search_prompts.call_args.kwargs["filter_string"] == (
            "catalog = 'main' AND schema = 'kasal'"
        )

    def test_load_resolves_the_newest_version_explicitly(self):
        """Never `load_prompt(name)` without a version: that asks the store for
        an alias called "latest", which Unity Catalog does not have."""
        client = _client(versions=(1, 3, 2))
        spec = jr.JudgeRegistry("http://x", client=client).load("acc")
        assert spec.version == 3
        client.load_prompt.assert_called_once_with(
            "kasal_judge__acc", version=3, allow_missing=True
        )

    def test_load_reads_unity_catalogs_version_response(self):
        client = _client(versions=(4, 12), uc=True)
        registry = jr.JudgeRegistry("databricks-uc", "main.kasal", client=client)
        spec = registry.load("acc")
        assert spec.version == 12
        client.load_prompt.assert_called_once_with(
            "main.kasal.kasal_judge__acc", version=12, allow_missing=True
        )

    def test_load_returns_none_when_missing(self):
        client = MagicMock()
        client.search_prompt_versions.return_value = []
        assert jr.JudgeRegistry("http://x", client=client).load("ghost") is None
        client.load_prompt.assert_not_called()
        # Unity Catalog raises for an unknown prompt instead of returning nothing.
        client.search_prompt_versions.side_effect = RuntimeError(
            "RESOURCE_DOES_NOT_EXIST: prompt does not exist"
        )
        assert (
            jr.JudgeRegistry("databricks-uc", "main.kasal", client=client).load("ghost")
            is None
        )

    def test_as_dict_is_what_the_dialog_reads(self):
        spec = jr.JudgeSpec("crew_88ab4478823c__acc", "x" * 5000, "qwen-30b", version=2)
        row = spec.as_dict()
        assert row["name"] == "acc" and row["full_name"] == "crew_88ab4478823c__acc"
        assert row["crew_id"] == "88ab4478823c" and row["model"] == "qwen-30b"
        assert len(row["instructions"]) == 4000


class TestWrites:
    def test_save_registers_a_tagged_version(self):
        client = MagicMock()
        client.register_prompt.return_value = _version("Rate {{ outputs }}.", version=4)
        registry = jr.JudgeRegistry("databricks-uc", "main.kasal", client=client)
        spec = registry.save(
            "crew_88ab4478823c__acc", "Rate {{ outputs }}.", "qwen-30b", "created"
        )
        assert spec.version == 4 and spec.model == "qwen-30b"
        kwargs = client.register_prompt.call_args.kwargs
        assert kwargs["name"] == "main.kasal.kasal_judge__crew_88ab4478823c__acc"
        assert kwargs["template"] == "Rate {{ outputs }}."
        assert kwargs["commit_message"] == "created"
        assert kwargs["tags"] == {
            jr.TAG_KIND: "judge",
            jr.TAG_MODEL: "qwen-30b",
            jr.TAG_CREW: "88ab4478823c",
        }

    def test_uc_permission_denial_becomes_the_grant_hint(self):
        client = MagicMock()
        client.register_prompt.side_effect = RuntimeError(
            "PERMISSION_DENIED: sp cannot create function"
        )
        registry = jr.JudgeRegistry("databricks-uc", "main.kasal", client=client)
        with pytest.raises(ValueError, match="MANAGE ON SCHEMA main.kasal"):
            registry.save("acc", "Rate {{ outputs }}.", "qwen-30b")
        # Locally the same text is not a UC grant problem: re-raised as is.
        local = jr.JudgeRegistry("http://x", client=client)
        with pytest.raises(RuntimeError):
            local.save("acc", "Rate {{ outputs }}.", "qwen-30b")

    def test_delete_is_false_for_a_missing_judge(self):
        client = MagicMock()
        client.search_prompt_versions.return_value = []
        assert jr.JudgeRegistry("http://x", client=client).delete("ghost") is False
        client.delete_prompt.assert_not_called()

    def test_delete_locally_drops_the_prompt_with_its_versions(self):
        client = _client(versions=(1, 2, 3))
        assert jr.JudgeRegistry("http://x", client=client).delete("acc") is True
        client.delete_prompt_version.assert_not_called()
        client.delete_prompt.assert_called_once_with("kasal_judge__acc")

    def test_delete_on_uc_removes_every_version_first_and_tolerates_gaps(self):
        client = _client(versions=(1, 3), uc=True)
        client.delete_prompt_version.side_effect = [
            None,
            RuntimeError("RESOURCE_DOES_NOT_EXIST: version 2 does not exist"),
            None,
        ]
        registry = jr.JudgeRegistry("databricks-uc", "main.kasal", client=client)
        assert registry.delete("acc") is True
        name = "main.kasal.kasal_judge__acc"
        assert [c.args for c in client.delete_prompt_version.call_args_list] == [
            (name, "1"),
            (name, "2"),
            (name, "3"),
        ]
        client.delete_prompt.assert_called_once_with(name)
