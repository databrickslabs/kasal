"""Which saved edit a HITL gate hands to the next crew when the flow resumes.

An edit saved while a run waits at a review gate (Config Editor "Save to
execution", Genie or Config "Save & Approve") is stored on the run's row as
``checkpoint_data.edited_config``. ExecutionHistoryService stamps it with the
approval the run was waiting at: ``edited_config_gate`` holds that approval's
id and gate node.

Every resume passes the run's approved gates again. A gate hands the edit on
only when the stamp names its own approval, so:

* an earlier gate's edit never replaces a later gate's output;
* an edit saved before a reject-and-retry is not applied to the new attempt
  (that attempt is a new approval);
* the edit is NOT cleared once read: when a later crew is retried, the flow
  passes this gate again and needs the same edit again.
"""

import json
from typing import Mapping, Optional

EDIT_KEY = "edited_config"
STAMP_KEY = "edited_config_gate"


def edit_for_gate(
    checkpoint_data: object, gate_node_id: str, approval_id: object
) -> Optional[str]:
    """The JSON this gate hands on, or None to hand on the crew's own output."""
    if not isinstance(checkpoint_data, Mapping):
        return None
    edited = checkpoint_data.get(EDIT_KEY)
    stamp = checkpoint_data.get(STAMP_KEY)
    if not edited or not isinstance(stamp, Mapping):
        return None
    if stamp.get("gate_node_id") != gate_node_id:
        return None
    if stamp.get("approval_id") != approval_id:
        return None
    return json.dumps(edited)
