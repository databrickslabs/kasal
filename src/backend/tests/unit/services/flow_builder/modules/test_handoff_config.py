"""Tests for handoff_config: the next crew's config_json after a pipeline config handoff.

A crew that sets switches in its own config_json (read mode, slicer parameters)
used to lose the whole handed-off pipeline config, because any non-empty
config_json was left untouched. The switches now stay on top of the handoff.
"""

import json

from src.services.flow_builder.modules.handoff_json import handoff_config

PIPELINE = {"join_key_map": {"dim_cal": "fiscper"}, "review_bar_pct": 90}


def test_empty_config_takes_the_handoff():
    for existing in ("", "{}", "null", None):
        assert json.loads(handoff_config(existing, PIPELINE)) == PIPELINE


def test_switches_stay_on_top_of_the_handoff():
    existing = json.dumps({"read_from_source_layer": True, "slicer_parameters": True})
    assert json.loads(handoff_config(existing, PIPELINE)) == {
        **PIPELINE,
        "read_from_source_layer": True,
        "slicer_parameters": True,
    }


def test_a_switch_wins_over_the_same_key_in_the_handoff():
    existing = json.dumps({"review_bar_pct": 95, "read_from_source_layer": True})
    assert json.loads(handoff_config(existing, PIPELINE))["review_bar_pct"] == 95


def test_a_pipeline_config_of_its_own_is_left_alone():
    existing = json.dumps({"join_key_map": {"dim_x": "x_id"}, "filter_sets": {}})
    assert handoff_config(existing, PIPELINE) is None


def test_text_that_is_not_a_json_object_is_left_alone():
    assert handoff_config("{pipeline_config_json}", PIPELINE) is None
    assert handoff_config('["read_from_source_layer"]', PIPELINE) is None
