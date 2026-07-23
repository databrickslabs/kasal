# Third-party notices

Kasal includes or conforms to third-party open-source work. Attributions below.

## A2UI (Agent-to-UI)

- Project: **google/A2UI** — [google/A2UI on GitHub](https://github.com/google/A2UI)
- License: **Apache License 2.0**
- Copyright: Google LLC and the A2UI contributors (incl. CopilotKit).

Kasal's "Predefined UI" feature **conforms to the A2UI v0.10 message protocol
and the "minimal" component catalog** (Text, Row, Column, Button, TextField).
We implement our own Kasal React renderer (themed with Kasal design tokens) over
the A2UI document format; we do not vendor A2UI's renderer source. The A2UI
specification/catalog JSON is reused under the terms of the Apache License 2.0.

A copy of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) is available
from the Apache Software Foundation.

### Apache 2.0 obligations honored
- This NOTICE preserves attribution to the A2UI project and its license.
- Any modifications to A2UI-derived schema/spec files (if vendored later) will
  be marked as changed, per Section 4 of the license.

## See also

- [Solution architecture guide](./ARCHITECTURE_GUIDE.md) — where the A2UI-conformant UI fits

Back to the [documentation hub](./README.md).
