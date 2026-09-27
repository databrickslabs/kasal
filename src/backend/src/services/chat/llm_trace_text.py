"""How a chat run's LLM request reads in its trace row."""


def messages_text(event: object) -> str:
    """The request messages of an LLM event as readable text.

    What 'LLM Request' → View shows: one ``role: content`` block per message.
    """
    msgs = getattr(event, "messages", None)
    if msgs is None:
        return ""
    if isinstance(msgs, str):
        return msgs
    if not isinstance(msgs, (list, tuple)):
        return str(msgs)
    return "\n\n".join(
        (
            f"{m.get('role', '?')}: {m.get('content', '')}"
            if isinstance(m, dict)
            else str(m)
        )
        for m in msgs
    )
