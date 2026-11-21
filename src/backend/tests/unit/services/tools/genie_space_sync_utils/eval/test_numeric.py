"""Numeric check: precision tolerance, percent/fraction forms, figure extraction."""

from src.services.tools.genie_space_sync_utils.eval.numeric import (
    expected_from_text,
    extract_numbers,
    figure,
    is_found,
    parse_number,
    relative_difference,
)


def test_a_figure_is_found_at_the_coarser_precision():
    assert is_found(figure(42.869), extract_numbers("about 42.9 units"))
    assert not is_found(figure(42.869), extract_numbers("43.1 units"))


def test_a_more_precise_text_must_round_to_the_expected_figure():
    assert is_found(figure(56.176), extract_numbers("56.1759"))
    assert not is_found(figure(56.176), extract_numbers("56.3"))


def test_a_bare_small_integer_cannot_stand_for_a_longer_decimal():
    assert not is_found(figure(3.4), extract_numbers("3"))


def test_fraction_and_percent_forms_match_each_other():
    assert is_found(figure(0.52727), extract_numbers("52.73%"))
    assert is_found(figure(57.415), extract_numbers("0.57415"))


def test_signs_and_thousands_separators_are_read():
    assert is_found(figure(-1125.0), extract_numbers("down 1,125"))


def test_expected_figures_from_words_skip_years_and_small_integers():
    values = [
        n.value for n in expected_from_text("In 2026, top 3 regions sold 1,250.5 units")
    ]
    assert values == [1250.5]


def test_parse_number_reads_whole_cells_only():
    assert parse_number("1234.50").decimals == 2
    assert parse_number("1.0E7").value == 1e7
    assert parse_number("North") is None
    assert parse_number("nan") is None


def test_relative_difference_is_zero_for_an_exact_match_and_none_without_numbers():
    assert relative_difference(figure(100), extract_numbers("100")) == 0.0
    assert relative_difference(figure(100), extract_numbers("105")) == 0.05
    assert relative_difference(figure(100), []) is None
