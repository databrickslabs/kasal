"""Kasal CALLING another A2A agent — the outbound half.

The protocol client and the registry of remotes an admin has attached. Every
request here goes to a URL somebody configured, which is why the client is
SSRF-checked per call and treats every response as untrusted input.
"""
