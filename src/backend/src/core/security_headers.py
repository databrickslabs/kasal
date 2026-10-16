"""HTTP security headers for every response Kasal serves, the SPA page included.

``SecurityHeadersMiddleware`` adds them to everything that reaches the FastAPI
app (API, ``/assets``, icons, docs). ``src/entrypoint.py``'s ``SPAMiddleware``
answers the page routes itself, outside that stack, so it sends
``PAGE_SECURITY_HEADERS`` on the document it serves.

The Content-Security-Policy is built from what the frontend actually loads:

- ``script-src 'unsafe-inline'``: the sandboxed srcdoc previews (diagrams,
  decks, HTML results) inherit the page's policy and run inline scripts.
  ``'wasm-unsafe-eval'``: the run-PDF export's layout engine is WebAssembly.
  No ``'unsafe-eval'``: the bundle never needs it, and leaving it out on purpose
  also blocks eval in the model-authored HTML those previews run (their own
  frame policies no longer ask for it).
- ``style-src 'unsafe-inline'``: MUI/emotion, Leaflet and the previews inject
  ``<style>`` elements and style attributes.
- ``img-src``: the app's own images, ``data:`` and ``blob:``, and the A2UI
  Map's OpenStreetMap tiles (bare host and subdomains). Nothing else: an image
  URL in model output must not load by itself, so the frontend shows it as a
  placeholder whose link opens it in a new tab (``shared/a2ui/lib/
  imagePolicy.ts``). If ``VITE_MAP_TILE_URL`` points the map at another tile
  host, add that host here; ``tests/unit/core/test_security_headers.py`` fails
  when the frontend's default tile URL is not allowed.
- ``font-src 'self' data:``: the app's fonts, and fonts embedded in previewed
  HTML (the approval dialog's frame allows ``data:`` fonts).
- ``connect-src``: the API and SSE streams (same origin), ``data:`` (the PDF
  engine reads its WebAssembly from a data URL), the PDF fonts, and the Power BI
  OAuth token endpoint the browser calls directly.
- ``frame-src 'self'``: the previews are srcdoc frames; nothing is framed from
  elsewhere. ``object-src 'none'``, ``base-uri 'self'``, ``form-action 'self'``.
- No ``frame-ancestors``, and no ``X-Frame-Options`` on the page: the Databricks
  workspace embeds Kasal cross-origin, and either would block that.
"""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

CONTENT_SECURITY_POLICY = "; ".join(
    [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: blob: https://tile.openstreetmap.org"
        " https://*.tile.openstreetmap.org",
        "font-src 'self' data:",
        "connect-src 'self' data: https://fonts.gstatic.com"
        " https://login.microsoftonline.com",
        "frame-src 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
    ]
)

#: Headers for the HTML document the SPA middleware serves.
PAGE_SECURITY_HEADERS: list[tuple[bytes, bytes]] = [
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode()),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"strict-origin-when-cross-origin"),
]

#: Headers for everything the FastAPI app answers. X-Frame-Options only matters
#: on a document; SAMEORIGIN here is kept from before.
SECURITY_HEADERS: list[tuple[bytes, bytes]] = [
    *PAGE_SECURITY_HEADERS,
    (b"x-frame-options", b"SAMEORIGIN"),
]


class SecurityHeadersMiddleware:
    """Pure ASGI middleware (not BaseHTTPMiddleware, which buffers streaming
    bodies and breaks SSE) that adds ``SECURITY_HEADERS`` to every response."""

    _CSP = CONTENT_SECURITY_POLICY
    _SECURITY_HEADERS = SECURITY_HEADERS

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_security_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(self._SECURITY_HEADERS)
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_security_headers)
