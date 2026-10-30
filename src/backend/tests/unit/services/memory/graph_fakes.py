"""Fakes shared by the graph-memory tests: a deterministic embedder and an LLM
that replies with scripted extractions."""

import hashlib
import json
import math
import re

DIM = 64


def embed(texts):
    """Bag-of-words hashing: texts sharing words score a higher cosine."""
    out = []
    for text in texts:
        vector = [0.0] * DIM
        for word in re.findall(r"\w+", text.lower()):
            vector[int(hashlib.md5(word.encode()).hexdigest(), 16) % DIM] += 1.0
        norm = math.sqrt(sum(x * x for x in vector)) or 1.0
        out.append([x / norm for x in vector])
    return out


def fact(
    subject, relation, obj, statement, single=False, object_type=None, kind="person"
):
    return {
        "subject": subject,
        "subject_type": kind,
        "relation": relation,
        "object": obj,
        "object_type": object_type,
        "single": single,
        "statement": statement,
    }


class ScriptedLLM:
    """``call`` returns the next scripted list of facts as an extraction reply."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = 0

    def call(self, messages):
        """A reply is a list of facts, or a dict to script the run note too."""
        self.calls += 1
        reply = self.replies.pop(0) if self.replies else []
        return json.dumps(reply if isinstance(reply, dict) else {"facts": reply})
