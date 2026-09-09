"""Connect tenant-supplied HTTP destinations only to validated public addresses.

Checking DNS before constructing an HTTP client is insufficient: its connector
resolves again. This backend validates the connector's own lookup and passes a
numeric address to the socket layer, retaining the original Host and TLS SNI.
"""

import asyncio
import socket
from typing import Iterable

import httpcore
import httpx
from aiohttp.abc import AbstractResolver, ResolveResult

from src.utils.url_security import UnsafeUrlError, _ip_is_private


class PublicNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self._backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            async with asyncio.timeout(timeout):
                addresses = await asyncio.get_running_loop().getaddrinfo(
                    host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
                )
                ips = list(dict.fromkeys(address[4][0] for address in addresses))
                if not ips or any(_ip_is_private(ip) for ip in ips):
                    raise UnsafeUrlError(
                        "Outbound destination resolves to a non-public address"
                    )
                last_error: httpcore.ConnectError | None = None
                for ip in ips:
                    try:
                        return await self._backend.connect_tcp(
                            ip,
                            port,
                            timeout=timeout,
                            local_address=local_address,
                            socket_options=socket_options,
                        )
                    except httpcore.ConnectError as error:
                        last_error = error
                raise last_error or httpcore.ConnectError("No reachable public address")
        except TimeoutError as error:
            raise httpcore.ConnectTimeout("Outbound connection timed out") from error
        except OSError as error:
            raise httpcore.ConnectError("Outbound DNS lookup failed") from error

    async def connect_unix_socket(
        self, *args: object, **kwargs: object
    ) -> httpcore.AsyncNetworkStream:
        raise UnsafeUrlError("Unix sockets are not allowed for outbound webhooks")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class PublicHTTPTransport(httpx.AsyncHTTPTransport):
    """HTTPX adapter using the public-address connector, without proxy bypasses.

    HTTPX exposes no network-backend constructor argument. Replacing the freshly
    constructed pool's backend is the only private integration seam; regression
    tests exercise an HTTP request through this adapter, including Host/TLS SNI.
    Clients MUST also set trust_env=False so environment proxy mounts cannot
    replace this transport. Certificate verification remains enabled.
    """

    def __init__(self) -> None:
        super().__init__(trust_env=False)
        self._pool._network_backend = PublicNetworkBackend()


class PublicResolver(AbstractResolver):
    """Aiohttp resolver: validate the addresses its TCP connector will use."""

    def __init__(self) -> None:
        from aiohttp.resolver import DefaultResolver

        self._resolver = DefaultResolver()

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        addresses = await self._resolver.resolve(host, port, family)
        if not addresses or any(
            _ip_is_private(address["host"]) for address in addresses
        ):
            raise UnsafeUrlError(
                "Outbound destination resolves to a non-public address"
            )
        return addresses

    async def close(self) -> None:
        await self._resolver.close()
