"""Tests for MCP CallToolResult normalization (mcp_handler._format_mcp_tool_result).

Results are flattened to agent-friendly text: structured JSON preferred, errors
surfaced, resource/image links kept as markdown image lines, inline binary
replaced with compact placeholders (never raw base64), text blocks newline-joined.
"""

import json
from types import SimpleNamespace

from src.engines.crewai.tools.mcp_handler import (
    _format_content_block,
    _format_mcp_tool_result,
)


def _text(t):
    return SimpleNamespace(type="text", text=t)


def _image(mime="image/png", data="QUJD"):  # base64-ish payload
    return SimpleNamespace(type="image", data=data, mimeType=mime)


def _resource_link(uri, name=None, mime=None):
    return SimpleNamespace(type="resource_link", uri=uri, name=name, mimeType=mime)


def _embedded(text=None, uri=None, mime=None, blob=None, name=None):
    return SimpleNamespace(
        type="resource",
        name=name,
        resource=SimpleNamespace(text=text, uri=uri, mimeType=mime, blob=blob),
    )


# --- single content blocks --------------------------------------------------


def test_text_block_returns_text():
    assert _format_content_block(_text("hello world")) == "hello world"


def test_inline_image_becomes_placeholder_not_base64():
    out = _format_content_block(_image("image/png", data="A" * 5000))
    assert out == "[image: image/png]"
    assert "AAAA" not in out  # the base64 payload is never included


def test_resource_link_image_is_markdown_image_line():
    out = _format_content_block(
        _resource_link("https://ws/img.png", name="Chart", mime="image/png")
    )
    assert out == "![Chart](https://ws/img.png)"


def test_resource_link_non_image_is_plain_link():
    out = _format_content_block(
        _resource_link("https://ws/doc.pdf", name="Doc", mime="application/pdf")
    )
    assert out == "[Doc](https://ws/doc.pdf)"


def test_embedded_resource_text_is_returned():
    out = _format_content_block(_embedded(text="file contents", uri="file:///a"))
    assert out == "file contents"


def test_embedded_resource_image_uri_is_image_line():
    out = _format_content_block(
        _embedded(uri="https://ws/i.png", mime="image/png", blob="x", name="Pic")
    )
    assert out == "![Pic](https://ws/i.png)"


def test_embedded_resource_blob_without_uri_is_placeholder():
    out = _format_content_block(_embedded(blob="x", mime="application/zip"))
    assert out == "[resource: application/zip]"


def test_unknown_block_returns_none():
    assert _format_content_block(SimpleNamespace(type="weird")) is None


# --- full results -----------------------------------------------------------


def test_multiple_blocks_newline_joined_and_image_preserved():
    result = SimpleNamespace(
        content=[
            _text("Here is the chart:"),
            _image(),
            _resource_link("https://ws/a.png", mime="image/png"),
        ],
        isError=False,
    )
    out = _format_mcp_tool_result(result)
    assert (
        out
        == "Here is the chart:\n[image: image/png]\n![https://ws/a.png](https://ws/a.png)"
    )


def test_structured_content_preferred_as_json():
    result = SimpleNamespace(
        structuredContent={"rows": [{"a": 1}], "count": 1},
        content=[_text("ignored when structured present")],
        isError=False,
    )
    out = _format_mcp_tool_result(result)
    assert json.loads(out) == {"rows": [{"a": 1}], "count": 1}


def test_error_flag_is_surfaced():
    result = SimpleNamespace(content=[_text("boom")], isError=True)
    assert _format_mcp_tool_result(result) == "Tool error: boom"


def test_empty_content_falls_back_to_str():
    result = SimpleNamespace(content=[], isError=False)
    out = _format_mcp_tool_result(result)
    assert isinstance(out, str) and out != ""


def test_none_result_is_empty_string():
    assert _format_mcp_tool_result(None) == ""


def test_plain_text_only_result_has_no_image_noise():
    result = SimpleNamespace(content=[_text("just text")], isError=False)
    assert _format_mcp_tool_result(result) == "just text"
