"""apply_diagram_directive: append the %md-sandbox diagram rule to a chat agent."""

from src.services.chat.diagram_directive import (
    DECK_TEMPLATE,
    DIAGRAM_DIRECTIVE,
    apply_diagram_directive,
)


def test_appends_directive_to_existing_backstory():
    spec = {"role": "Assistant", "backstory": "You are helpful."}
    apply_diagram_directive(spec)
    assert spec["backstory"].startswith("You are helpful.")
    assert "%md-sandbox diagram specialist" in spec["backstory"]
    assert "```html" in spec["backstory"]


def test_handles_missing_backstory():
    spec = {"role": "Assistant"}
    apply_diagram_directive(spec)
    assert spec["backstory"] == DIAGRAM_DIRECTIVE


def test_directive_covers_the_slide_deck_contract():
    # Slides/presentations must be emitted as <section class="slide">.
    assert 'class="slide"' in DIAGRAM_DIRECTIVE
    # The rich, sized template lives in DECK_TEMPLATE.
    assert 'class="slide"' in DECK_TEMPLATE
    assert "1280px" in DECK_TEMPLATE and "720px" in DECK_TEMPLATE


def test_deck_template_added_only_for_presentation_requests():
    # A presentation request gets the rich example deck…
    deck = apply_diagram_directive({"backstory": ""}, "create a presentation on X")
    assert "SLIDE DECK DESIGN SYSTEM" in deck["backstory"]
    # …an ordinary chat turn does not (keeps the prompt lean).
    plain = apply_diagram_directive({"backstory": ""}, "what is a knowledge graph")
    assert "SLIDE DECK DESIGN SYSTEM" not in plain["backstory"]
    assert "%md-sandbox diagram specialist" in plain["backstory"]


def test_is_idempotent():
    spec = {"backstory": "base"}
    apply_diagram_directive(spec)
    once = spec["backstory"]
    apply_diagram_directive(spec)
    assert spec["backstory"] == once  # not appended twice


def test_returns_same_dict():
    spec = {"backstory": ""}
    assert apply_diagram_directive(spec) is spec


class TestPaletteDrivenTemplate:
    def test_workspace_palette_colors_the_template(self):
        from src.services.chat.diagram_directive import apply_diagram_directive

        spec = {"backstory": ""}
        themes = {
            "presentation": {
                "accent": "#AB47BC",
                "background": "#1A1B2E",
                "surface": "#F5F2FA",
                "heading": "#241C33",
                "text": "#4A4458",
                "muted": "#8A7F9E",
            }
        }
        apply_diagram_directive(spec, "create a presentation on x", themes=themes)
        story = spec["backstory"]
        assert "#AB47BC" in story  # workspace accent, not the default
        assert "#1A1B2E" in story  # workspace cover background
        assert "#FF5F46" not in story  # the default accent must be gone

    def test_default_palette_without_themes(self):
        from src.services.chat.diagram_directive import apply_diagram_directive

        spec = {"backstory": ""}
        apply_diagram_directive(spec, "create a presentation on x", themes=None)
        assert "#FF5F46" in spec["backstory"]

    def test_default_theme_key_falls_back(self):
        from src.services.chat.diagram_directive import apply_diagram_directive

        spec = {"backstory": ""}
        apply_diagram_directive(
            spec, "make me slides", themes={"default": {"accent": "#00C853"}}
        )
        assert "#00C853" in spec["backstory"]

    def test_light_cover_background_gets_dark_text(self):
        from src.services.chat.diagram_directive import deck_template

        t = deck_template({"background": "#FFFFFF"})
        # The cover must not render white-on-white.
        assert "color:#101418" in t

    def test_incidental_mention_gets_no_deck_template(self):
        from src.services.chat.diagram_directive import (
            _DECK_MARKER,
            apply_diagram_directive,
        )

        spec = {"backstory": ""}
        apply_diagram_directive(spec, "how do slide-out panels work?")
        assert _DECK_MARKER not in spec["backstory"]


class TestPaletteHardening:
    def test_non_dict_palette_never_crashes(self):
        from src.services.chat.diagram_directive import deck_template

        for bad in ("blue", 42, ["#fff"], True):
            assert "SLIDE DECK" in deck_template(bad)  # falls back to defaults

    def test_non_color_values_are_rejected(self):
        from src.services.chat.diagram_directive import deck_template

        t = deck_template({"accent": '#f00" onload="x'})
        assert "onload" not in t  # a stray quote must not escape a style attr
        assert "#FF5F46" in t  # default accent used instead

    def test_named_css_colors_are_accepted(self):
        from src.services.chat.diagram_directive import deck_template

        assert "rebeccapurple" in deck_template({"accent": "rebeccapurple"})


class TestDirectiveScope:
    def test_a2ui_owned_deliverables_are_excluded_from_the_html_block(self):
        # A map request must produce DATA for the app's native Leaflet Map —
        # not a hand-drawn ```html approximation that makes A2UI yield.
        from src.services.chat.diagram_directive import DIAGRAM_DIRECTIVE

        assert "NEVER use the ```html block for maps" in DIAGRAM_DIRECTIVE
        assert "dashboards" in DIAGRAM_DIRECTIVE
        # Structured text, not drawings: an ASCII-art mindmap in a code fence
        # is unparseable input for the composer and killed the Mindmap surface.
        assert "STRUCTURED markdown" in DIAGRAM_DIRECTIVE
        assert "NEVER draw ASCII art" in DIAGRAM_DIRECTIVE
