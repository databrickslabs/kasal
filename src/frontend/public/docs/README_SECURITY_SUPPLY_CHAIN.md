# Supply Chain Security: Threat Landscape & Future Work

**Context:** On March 24, 2026, Endor Labs identified that `litellm` versions 1.82.7 and 1.82.8
on PyPI contained malicious code injected by the threat actor **TeamPCP**. litellm is a core
Kasal dependency used for LLM provider routing. This document analyses the threat, assesses
Kasal's current exposure, and proposes concrete mitigations as future work.

Related documents:
- **README_SECURITY_COMPLIANCE.md** — prompt injection and agentic security guardrails
- **README_SECURITY_GUARDRAILS_TESTGUIDE.md** — manual and automated test guide

---

## The litellm / TeamPCP Incident

### What Happened

`litellm==1.82.7` and `1.82.8` were published to PyPI on March 24, 2026 with a backdoor
injected into `litellm/proxy/proxy_server.py` at lines 128–139 — code that does not exist in
the upstream GitHub repository at the corresponding commit. The injection fires at **module
import time**, meaning any process that imports litellm triggers the payload with no additional
user interaction.

Version 1.82.8 added a second vector: a `litellm_init.pth` file placed in `site-packages` that
Python's `site.py` processes automatically at **every interpreter startup**, even if litellm is
never explicitly imported.

### What the Payload Does

The attack runs in three stages:

1. **Credential harvester** — sweeps the host for SSH keys, AWS/GCP/Azure credentials,
   Kubernetes secrets, `.env` files (recursively, 6 dirs deep), database passwords, crypto
   wallets, TLS private keys, CI/CD secrets, and shell history. Exfiltrates everything to
   `models.litellm.cloud` encrypted with RSA-4096 + AES-256-CBC.

2. **Kubernetes lateral movement** — if a K8s service account token is present, deploys a
   privileged pod (`hostPID`, `hostNetwork`, full host filesystem mount) to every node in the
   cluster, including control-plane nodes.

3. **Persistent backdoor** — installs `~/.config/sysmon/sysmon.py` as a systemd user service
   ("System Telemetry Service") that polls `checkmarx.zone/raw` every 50 minutes and executes
   downloaded binaries.

### Attribution: TeamPCP

TeamPCP is the threat actor behind a month-long campaign spanning five ecosystems:

| Date | Target | Method |
|------|--------|--------|
| Feb 28 | Aqua Trivy | Pwn Request workflow — stole PAT |
| Mar 19 | Aqua Trivy (again) | Residual access from incomplete remediation |
| Mar 20 | npm (45+ packages) | Self-propagating worm via stolen npm tokens |
| Mar 22 | Docker Hub | Stolen Aqua Docker Hub credentials |
| Mar 23 | Checkmarx KICS + OpenVSX | Compromised service accounts |
| Mar 24 | **litellm PyPI** | Compromised PyPI publishing credentials |

The pattern is deliberate: each compromised environment yields credentials that unlock the next
target. TeamPCP exclusively targets **security-adjacent tooling** (vulnerability scanners, IaC
analyzers, LLM proxies) — tools that run with elevated privileges by design and are therefore
maximally efficient to compromise.

---

## Kasal's Current Exposure

### Are we directly affected?

**No.** Kasal pins litellm to an exact version:

```
litellm==1.74.9   # in src/requirements.txt and pyproject.toml
```

`1.74.9` predates the compromise window (`1.82.7`/`1.82.8` published March 24, 2026).
As long as the lock is not bumped past `1.82.6` (the last known-clean release), Kasal is not
directly exposed.

### Would our existing security guardrails have caught it?

**No — they solve a different problem.** Kasal's security work (Phases 1–5, documented in
`README_SECURITY_COMPLIANCE.md`) protects against **prompt injection attacks**: malicious
instructions embedded in data that LLM agents process at runtime.

The litellm attack is a **supply chain compromise**: malicious code injected into a Python
dependency that executes before any of our application code runs. The two threat categories
require distinct defenses:

| Kasal guardrail | Protects against | Catches supply chain attack? |
|----------------|-----------------|------------------------------|
| Prompt injection detector (regex) | Injection in LLM inputs/outputs | ❌ fires before our code runs |
| Secret leak detector | Credentials in agent outputs | ❌ exfiltration goes direct HTTPS, never through scan pipeline |
| Trifecta / mixed-task detection | Dangerous tool combinations | ❌ architecture-level, not dependency-level |
| LLM injection guardrail | Injection in task output | ❌ task pipeline never reached |
| Security headers / CSP | XSS, clickjacking in browser | ❌ irrelevant to this threat |

**Partial indirect mitigations already in place:**
- OBO authentication means Kasal does not store long-lived Databricks tokens in `.env` files —
  reducing what the credential harvester would find
- API keys are stored in the database encrypted, not as plain environment variables
- The version pin at `1.74.9` is exact (`==`), not a range (`>=`)

---

## Threat Model Gap: Supply Chain Attacks

The current Kasal security model has **no defenses in the dependency supply chain layer**.
This is the gap. The attack surface includes:

- PyPI packages (Python backend: ~80 direct deps, ~400 transitive)
- npm packages (React frontend: ~600 direct deps)
- GitHub Actions (CI/CD pipelines)
- Docker base images

TeamPCP has now demonstrated active targeting of all four of these surfaces.

---

## Future Work: Proposed Mitigations

The proposals below are grouped by implementation effort. None are currently implemented.

---

### Tier 1 — Low Effort, High Impact (hours of work)

#### 1.1 Hash Pinning in requirements.txt

**Problem:** `litellm==1.74.9` pins the version but not the wheel content. PyPI allows a
maintainer to re-upload different content for the same version. TeamPCP regenerated the wheel's
`RECORD` file with the backdoored content's hash — standard `pip install` integrity checks pass.

**Proposal:** Add `--hash` constraints to `requirements.txt` using `pip-compile`:

```bash
pip-compile --generate-hashes requirements.in > requirements.txt
```

This produces entries like:
```
litellm==1.74.9 \
    --hash=sha256:abc123... \
    --hash=sha256:def456...
```

`pip install --require-hashes` then refuses to install any wheel whose hash doesn't match —
even if the version string is identical.

**Benefit:** Full stop against version re-upload attacks. Zero runtime overhead.

---

#### 1.2 `.pth` File Integrity Check at Container Start

**Problem:** `litellm==1.82.8` drops a `litellm_init.pth` file in `site-packages` that fires on
every Python invocation. This vector is invisible to version checks — only a file system scan
catches it.

**Proposal:** Add a startup assertion to `src/entrypoint.py` (or a Docker `ENTRYPOINT` script):

```python
import glob, site, sys

pth_files = glob.glob(f"{site.getsitepackages()[0]}/*.pth")
unexpected = [f for f in pth_files if "distutils" not in f and "easy-install" not in f]
if unexpected:
    sys.exit(f"[SECURITY] Unexpected .pth file(s) detected: {unexpected}. Aborting startup.")
```

**Benefit:** Catches the `1.82.8`-style `.pth` injection vector before the app accepts traffic.

---

#### 1.3 litellm Source Integrity Check at Boot

**Problem:** The injected code in `proxy_server.py` is not detectable from the version number
alone — it requires comparing file content against the known-good source.

**Proposal:** Hash the installed `proxy_server.py` at startup and compare against a pinned
expected value:

```python
import hashlib, importlib.util, sys

spec = importlib.util.find_spec("litellm.proxy.proxy_server")
if spec and spec.origin:
    with open(spec.origin, "rb") as f:
        actual_hash = hashlib.sha256(f.read()).hexdigest()
    expected_hash = "KNOWN_GOOD_SHA256_OF_1_74_9"
    if actual_hash != expected_hash:
        sys.exit(f"[SECURITY] litellm proxy_server.py hash mismatch. Aborting.")
```

This could be extended to a manifest of critical files across all security-relevant dependencies.

**Benefit:** Catches any wheel-level tampering of litellm regardless of version number.

---

#### 1.4 Dependency Diff in PR Review

**Problem:** When a developer bumps litellm from `1.74.9` to `1.82.x`, the PR shows one line
changed in `requirements.txt`. There is no automatic visibility into what changed in the package
itself, what new transitive dependencies were introduced, or whether the maintainer account
was recently compromised.

**Proposal:** A PR check (GitHub Action) that on any change to `requirements.txt` or
`pyproject.toml`:
1. Renders a human-readable diff of added/changed/removed packages including transitive deps
2. Links each bumped package to its PyPI changelog and recent commit history
3. Flags packages where the maintainer account changed recently (detectable via PyPI API)
4. Blocks merge if any dependency was published less than 7 days ago (mirroring Databricks'
   npm proxy policy)

---

### Tier 2 — Medium Effort, High Impact (days of work)

#### 2.1 PyPI Internal Proxy (mirror the npm policy)

**Problem:** Databricks has already mandated an npm proxy (`npm-proxy.dev.databricks.com`)
that blocks packages newer than 7 days. PyPI has no equivalent yet — packages are fetched
directly from pypi.org with no hold period or malware scan.

**Proposal:** Extend the same architecture to PyPI. A proxy/mirror that:
- Holds all new package versions for 7 days before serving them
- Runs automated malware scanning (Endor Labs / Snyk / VirusTotal) on the held version
- Blocks versions that fail scanning even after the hold period
- Provides a fast-track bypass for critical security patches with manual approval

`litellm==1.82.7` and `1.82.8` were published and removed from PyPI within hours. A 7-day hold
would have been a complete stop.

**Note:** This is the single highest-leverage mitigation for the whole class of PyPI supply chain
attacks, not just litellm.

---

#### 2.2 Network Egress Policy

**Problem:** Even if the payload runs, the exfiltration step requires an outbound HTTPS POST to
`models.litellm.cloud`. In a Kubernetes deployment, this can be blocked at the network layer.

**Proposal:** A Kubernetes `NetworkPolicy` that restricts egress from Kasal pods to a whitelist:
- Databricks workspace endpoints
- Known LLM provider APIs (OpenAI, Anthropic, etc.)
- Internal cluster services

Any outbound connection to an unlisted domain (like `models.litellm.cloud`) is dropped silently.
This does not prevent the credential harvest from running locally, but it prevents exfiltration
— the attacker gets no data.

For the Databricks Apps deployment specifically, this would be a `app.yaml`-level network
restriction.

---

#### 2.3 Extend SecretLeakDetector to Environment Variables at Startup

**Problem:** The existing `SecretLeakDetector` scans agent outputs for leaked credentials. It
does not scan the runtime environment itself. If Kasal is ever deployed with overly permissive
credentials in env vars (e.g. a wildcard AWS key, a long-lived Databricks PAT), there is no
automated detection of that misconfiguration.

**Proposal:** Run `SecretLeakDetector` once at startup against `os.environ`:

```python
from src.engines.crewai.security.secret_leak_detector import detect

findings = detect(" ".join(f"{k}={v}" for k, v in os.environ.items()))
if findings.detected:
    logger.warning(
        "[SECURITY] Overly permissive credentials detected in environment: %s. "
        "Consider moving to OBO auth or Databricks Secrets.",
        findings.secret_types
    )
```

This turns the existing detector into a configuration hygiene check, surfacing credential
misconfigurations before they become blast radius.

---

#### 2.4 Full Dependency Lock File with Transitive Hashes

**Problem:** `requirements.txt` pins direct dependencies but not the full transitive graph.
litellm itself has ~40 transitive dependencies — any of those could be the vector in a future
attack. `openai`, `anthropic`, `httpx`, `pydantic` are all high-value targets.

**Proposal:** Adopt `uv` or `pip-compile` with `--generate-hashes` to produce a complete
lock file covering every package in the dependency graph. This is stricter than Tier 1.1 (which
only hashes direct dependencies) and covers the full attack surface.

---

### Tier 3 — Significant Effort (weeks of work)

#### 3.1 Runtime Behavioral Monitoring (eBPF / Falco)

**Proposal:** Deploy Falco (or Tetragon) in the Kasal Kubernetes cluster with rules that alert on:

```yaml
# Credential file reads
- rule: Sensitive File Read
  condition: open_read and fd.name in (~/.aws/credentials, ~/.ssh/id_rsa, /etc/shadow)

# Unexpected subprocess from import time
- rule: Python Subprocess at Import
  condition: spawned_process and proc.name=python and proc.pname=python and proc.cmdline contains "proxy_server"

# New .pth file in site-packages
- rule: New PTH File
  condition: create and fd.name endswith ".pth" and fd.directory contains "site-packages"

# Privileged pod creation
- rule: Privileged Pod in kube-system
  condition: k8s_audit and ka.verb=create and ka.target.namespace=kube-system
```

**Benefit:** Detects the attack *while it is running* even if it bypasses all static checks.

---

#### 3.2 Isolated Subprocess for litellm

**Problem:** litellm runs in the same Python process as Kasal's application code, meaning a
compromised litellm has full access to all loaded secrets, tokens, and file system paths.

**Proposal:** Run litellm in a subprocess with a restricted environment:
- No access to SSH keys (`HOME` unset or redirected to empty tmpdir)
- No AWS/GCP/Azure credential files mounted
- Network access restricted to LLM provider endpoints only
- Communicate with the main process via a local socket or shared memory

This is the most engineering-intensive option but provides the strongest isolation guarantee:
a compromised litellm can only steal what was explicitly passed to it.

---

#### 3.3 SBOM Generation and Continuous Monitoring

**Proposal:** Generate a Software Bill of Materials (SBOM) for every Kasal release in CycloneDX
or SPDX format, and subscribe it to a vulnerability feed (Endor Labs, Snyk, OSV). When a new
vulnerability or compromise is announced for any dependency, the feed triggers an alert within
minutes rather than waiting for a developer to notice a blog post.

This would have surfaced the litellm compromise within hours of the Endor Labs disclosure,
rather than requiring manual awareness.

---

## Indicators of Compromise (IoCs)

If you suspect a compromised litellm was ever installed in any Kasal environment, check for:

```bash
# Check installed version
pip show litellm | grep Version

# Check for .pth payload (1.82.8 only)
find "$(python3 -c 'import site; print(site.getsitepackages()[0])')" -name "litellm_init.pth"

# Check for persistence artifacts
ls ~/.config/sysmon/sysmon.py 2>/dev/null
ls ~/.config/systemd/user/sysmon.service 2>/dev/null
ls /tmp/pglog /tmp/.pg_state 2>/dev/null
systemctl --user status sysmon.service 2>/dev/null

# Check for attacker pods in Kubernetes
kubectl get pods -n kube-system | grep node-setup

# Check network logs for C2 domains
# models.litellm.cloud  — exfiltration endpoint
# checkmarx.zone        — persistence / binary delivery
```

Compromised wheel hashes (do not install):
```
litellm-1.82.7: sha256=8395c3268d5c5dbae1c7c6d4bb3c318c752ba4608cfcd90eb97ffb94a910eac2
litellm-1.82.8: sha256=d2a0d5f564628773b6af7b9c11f6b86531a875bd2d186d7081ab62748a800ebb
```

Last known-clean version: `litellm==1.82.6` (published 2026-03-22, verified by Endor Labs).

---

## Summary: Priority Stack Rank

| # | Proposal | Effort | Impact | Covers |
|---|----------|--------|--------|--------|
| 1 | PyPI internal proxy (7-day hold) | Medium | 🔴 Highest | Whole PyPI attack class |
| 2 | Hash pinning (`--require-hashes`) | Low | 🔴 High | Re-upload attacks |
| 3 | `.pth` file check at startup | Low | 🔴 High | `.pth` injection vector |
| 4 | litellm source integrity check at boot | Low | 🟠 High | Tampered wheel detection |
| 5 | Network egress policy | Medium | 🟠 High | Blocks exfiltration even if payload runs |
| 6 | Dependency diff in PR review | Medium | 🟠 Medium | Visibility on bumps |
| 7 | Extend SecretLeakDetector to env vars | Low | 🟡 Medium | Configuration hygiene |
| 8 | Full transitive lock file | Medium | 🟠 High | Transitive dep attacks |
| 9 | Runtime monitoring (Falco) | High | 🔴 High | Detection while running |
| 10 | Isolated litellm subprocess | High | 🟠 Medium | Blast radius reduction |
| 11 | SBOM + continuous monitoring | High | 🟠 High | Early disclosure awareness |

**The single most impactful near-term action:** a PyPI proxy with the same 7-day hold policy
Databricks has already mandated for npm. It is the supply chain equivalent of what our prompt
injection guardrails do for agentic attacks — a structural defense that applies to the whole
class, not just the specific known instance.
