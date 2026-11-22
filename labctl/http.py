# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""A small HTTP(S) client for lab endpoints (standard library only).

Why not urllib.request:
- The host cannot resolve `*.lab.test` (only pods can, through the CoreDNS
  rewrite `lab up` installs). `connect_to` dials a different address while
  TLS still verifies the certificate against the URL's host name, like
  `curl --connect-to`.
- Every request verifies against the lab CA. There is no "skip verify".
- Redirects are never followed: a redirect would carry an Authorization
  header to a host the caller never chose.
"""

from __future__ import annotations

import http.client
import json
import socket
import ssl
import urllib.parse
from dataclasses import dataclass, field
from typing import Mapping, Optional, Tuple, Union

Body = Union[None, bytes, str, dict, list]


class HTTPError(Exception):
    def __init__(self, message: str, status: int = 0, body: bytes = b""):
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass
class Response:
    status: int
    body: bytes
    headers: dict = field(default_factory=dict)

    def json(self):
        return json.loads(self.body or b"null")

    def text(self, limit: int = 2000) -> str:
        return self.body[:limit].decode("utf-8", errors="replace")


def tls_context(cafile: Optional[str]) -> ssl.SSLContext:
    """Verify against `cafile` only (the lab CA), TLS 1.2 or newer."""
    ctx = ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


class _HTTPS(http.client.HTTPSConnection):
    def __init__(self, host, port, target, context, timeout):
        super().__init__(host, port, context=context, timeout=timeout)
        self._target = target
        self._ctx = context

    def connect(self):
        sock = socket.create_connection(self._target or (self.host, self.port), self.timeout)
        self.sock = self._ctx.wrap_socket(sock, server_hostname=self.host)


class _HTTP(http.client.HTTPConnection):
    def __init__(self, host, port, target, timeout):
        super().__init__(host, port, timeout=timeout)
        self._target = target

    def connect(self):
        self.sock = socket.create_connection(self._target or (self.host, self.port), self.timeout)


def request(
    method: str,
    url: str,
    *,
    cafile: Optional[str] = None,
    connect_to: Optional[Tuple[str, int]] = None,
    headers: Optional[Mapping[str, str]] = None,
    body: Body = None,
    form: Optional[Mapping[str, str]] = None,
    timeout: float = 30,
) -> Response:
    """Send one request; return the response whatever its status.

    Raises HTTPError only when no response arrived (DNS, TCP, TLS, timeout).
    """
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("https", "http") or not parts.hostname:
        raise HTTPError(f"unsupported URL {url!r}")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    hdrs = {"Accept": "application/json", "User-Agent": "fluid-fabric-lab"}
    hdrs.update(headers or {})
    data: Optional[bytes]
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")
    elif isinstance(body, (dict, list)):
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    elif isinstance(body, str):
        data = body.encode()
    else:
        data = body
    if parts.scheme == "https":
        conn = _HTTPS(parts.hostname, port, connect_to, tls_context(cafile), timeout)
    else:
        conn = _HTTP(parts.hostname, port, connect_to, timeout)
    try:
        conn.request(method, path, body=data, headers=hdrs)
        resp = conn.getresponse()
        payload = resp.read()
        return Response(resp.status, payload, {k.lower(): v for k, v in resp.getheaders()})
    except (OSError, http.client.HTTPException) as exc:  # ssl.SSLError is an OSError
        raise HTTPError(f"{method} {parts.scheme}://{parts.hostname}:{port}{parts.path}: {exc}") from exc
    finally:
        conn.close()
