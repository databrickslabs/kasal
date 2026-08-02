"""What a flow HITL gate must tell the approver.

The chat approval card is shared with tool approval, so the gate has to say
which kind it is — see the class below.
"""


class TestTheGateSaysWhatItIs:
    """The approval card is SHARED with tool approval, so a gate must identify itself.

    Without `kind`, the card falls back to its tool_call default and tells the
    reader "✋ The agent wants to run a tool" — for a flow paused between two
    crews, where no agent and no tool are involved. The observed gate_config was
    only {message, timeout_seconds, timeout_action, require_comment,
    allowed_approvers}: nothing said it was a flow gate, or which step it gates.
    """

    @staticmethod
    def _gate_configs(source: str):
        """Every gate_config literal built in flow_builder, as parsed dicts."""
        import ast
        import pathlib

        tree = ast.parse(pathlib.Path(source).read_text())
        out = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            target = node.targets[0]
            if not (isinstance(target, ast.Name) and target.id == "gate_config"):
                continue
            if not isinstance(node.value, ast.Dict):
                continue
            out.append(
                {
                    k.value
                    for k in node.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
            )
        return out

    def test_every_gate_declares_its_kind_and_step(self):
        configs = self._gate_configs(
            "src/services/flow_builder/modules/flow_builder.py"
        )

        assert configs, "no gate_config literals found — did the builder move?"
        for keys in configs:
            assert "kind" in keys, f"a gate_config omits 'kind': {sorted(keys)}"
            assert (
                "step_name" in keys
            ), f"a gate_config omits 'step_name': {sorted(keys)}"
