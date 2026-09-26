"""Audit M3: logging a GroupContext must never write the user's bearer token."""

from src.utils.user_context import GroupContext


def test_group_context_repr_omits_access_token_and_user():
    ctx = GroupContext(
        group_ids=["g1"],
        group_email="a@example.com",
        access_token="dapi-secret-token-value",
        current_user=object(),
    )
    text = f"{ctx} {ctx!r} {str(ctx)}"

    assert "dapi-secret-token-value" not in text
    assert "current_user" not in text
    assert "g1" in text  # the useful fields are still there
    assert ctx.access_token == "dapi-secret-token-value"
