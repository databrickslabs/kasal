"""Translate the shared agent conversation to Anthropic's native Messages API."""

import json


def content_blocks(content):
    if isinstance(content, str):
        return [{"type": "text", "text": content}] if content else []
    result = []
    for block in content or []:
        if block.get("type") in (
            "text",
            "thinking",
            "redacted_thinking",
            "document",
            "image",
        ):
            result.append(block)
        elif block.get("type") == "image_url":
            url = block["image_url"]
            url = url["url"] if isinstance(url, dict) else url
            if url.startswith("data:"):
                header, data = url.split(",", 1)
                source = {
                    "type": "base64",
                    "media_type": header[5:].split(";")[0],
                    "data": data,
                }
            else:
                source = {"type": "url", "url": url}
            result.append({"type": "image", "source": source})
        else:
            raise ValueError(
                f"Unsupported Anthropic content block: {block.get('type')}"
            )
    return result


def message_params(params, signed_blocks):
    system, messages = [], []
    for message in params["messages"]:
        role = message["role"]
        if role in ("system", "developer"):
            system.extend(content_blocks(message.get("content")))
            continue
        if role == "tool":
            role = "user"
            blocks = [
                {
                    "type": "tool_result",
                    "tool_use_id": message["tool_call_id"],
                    "content": message.get("content") or "",
                }
            ]
        else:
            calls = message.get("tool_calls") or []
            ids = tuple(call["id"] for call in calls)
            # Replay the exact signed thinking + tool-use blocks on subsequent rounds.
            # Reconstructing just the visible text drops signatures and invalidates tools.
            blocks = signed_blocks.get(ids) if ids else None
            if blocks is None:
                blocks = content_blocks(message.get("content"))
                for call in calls:
                    fn = call["function"]
                    blocks.append(
                        {
                            "type": "tool_use",
                            "id": call["id"],
                            "name": fn["name"],
                            "input": json.loads(fn["arguments"]),
                        }
                    )
        if not blocks:
            continue
        if messages and messages[-1]["role"] == role:
            messages[-1]["content"].extend(blocks)
        else:
            messages.append({"role": role, "content": list(blocks)})
    result = {
        "model": params["model"],
        "messages": messages,
        "max_tokens": params.get("max_completion_tokens")
        or params.get("max_tokens")
        or 4096,
    }
    if system:
        result["system"] = system
    for name in ("timeout", "extra_headers"):
        if params.get(name) is not None:
            result[name] = params[name]
    if params.get("stop"):
        stop = params["stop"]
        result["stop_sequences"] = [stop] if isinstance(stop, str) else stop
    if params.get("tools"):
        result["tools"] = [
            {
                "name": t["function"]["name"],
                "description": t["function"].get("description", ""),
                "input_schema": t["function"].get(
                    "parameters", {"type": "object", "properties": {}}
                ),
            }
            for t in params["tools"]
        ]
    choice = params.get("tool_choice")
    if choice:
        if isinstance(choice, dict):
            result["tool_choice"] = {"type": "tool", "name": choice["function"]["name"]}
        else:
            result["tool_choice"] = {"type": "any" if choice == "required" else choice}
    if params.get("parallel_tool_calls") is False and result.get("tools"):
        result.setdefault("tool_choice", {"type": "auto"})[
            "disable_parallel_tool_use"
        ] = True
    # Only native request extensions cross this boundary; never send OpenAI-only fields.
    extra = params.get("extra_body") or {}
    for name in ("thinking", "output_config", "metadata", "service_tier"):
        if name in extra:
            result[name] = extra[name]
    # New SDK versions omit legacy sampling arguments from their typed surface.
    # Older non-thinking models still accept them in the native request body.
    # Manual thinking requires default sampling, so omit overrides when thinking.
    if not result.get("thinking"):
        sampling = {
            k: params[k] for k in ("temperature", "top_p") if params.get(k) is not None
        }
        if sampling:
            result["extra_body"] = sampling
    output = params.get("response_format") or {}
    if output.get("type") == "json_schema":
        result["output_config"] = {
            **result.get("output_config", {}),
            "format": {
                "type": "json_schema",
                "schema": output["json_schema"]["schema"],
            },
        }
    return result
