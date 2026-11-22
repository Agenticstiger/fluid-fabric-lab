# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Path-style S3 object calls on SeaweedFS, signed with AWS Signature Version 4 (standard library only).

The launcher is standard-library only and the host has no boto3, so the few
object calls `lab up` and `lab check` make on bucket lab-airflow (put, get,
delete, list) are signed here. The signing follows AWS's "Authenticating
Requests: Using the Authorization Header (AWS Signature Version 4)" and is
tested against that page's worked example. It reaches SeaweedFS the way every
other lab call from the host does, through the Gateway port-forward with the
real `s3.lab.test` name (labctl.bootstrap.LabClient.https).

Keys are never logged; only a request's method, bucket and key reach a message.
"""

from __future__ import annotations

import datetime
import hashlib
import hmac
import re
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Optional

from labctl import http

REGION = "us-east-1"
SERVICE = "s3"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class S3Error(Exception):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _quote(text: str) -> str:
    return urllib.parse.quote(text, safe="-_.~")


def canonical_query(params: dict) -> str:
    return "&".join(f"{_quote(k)}={_quote(str(v))}" for k, v in sorted(params.items()))


def sign(method: str, host: str, path: str, params: dict, headers: dict, payload_sha256: str, access: str,
         secret: str, when: datetime.datetime, region: str = REGION, service: str = SERVICE) -> dict:
    """The headers to send: `headers` plus x-amz-date, x-amz-content-sha256, Host and Authorization.

    `path` is the request path as sent (already percent-encoded once; S3 signs it as is).
    """
    stamp = when.strftime("%Y%m%dT%H%M%SZ")
    day = stamp[:8]
    signed = {k.lower(): re.sub(r"\s+", " ", str(v).strip()) for k, v in headers.items()}
    signed.update({"host": host, "x-amz-content-sha256": payload_sha256, "x-amz-date": stamp})
    names = ";".join(sorted(signed))
    canonical_headers = "".join(f"{k}:{signed[k]}\n" for k in sorted(signed))
    request = "\n".join([method, path, canonical_query(params), canonical_headers, names, payload_sha256])
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", stamp, scope, hashlib.sha256(request.encode()).hexdigest()])
    key = _hmac(_hmac(_hmac(_hmac(("AWS4" + secret).encode(), day), region), service), "aws4_request")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    out = dict(headers)
    out.update({"Host": host, "x-amz-date": stamp, "x-amz-content-sha256": payload_sha256,
                "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope},SignedHeaders={names},"
                                 f"Signature={signature}"})
    return out


@dataclass
class Bucket:
    """One bucket on SeaweedFS, as one identity. `send` is `LabClient.https` (or a fake)."""

    send: Callable
    name: str
    access: str
    secret: str
    host: str = "s3.lab.test"
    clock: Callable = lambda: datetime.datetime.now(datetime.timezone.utc)

    def _call(self, method: str, key: str = "", params: Optional[dict] = None, body: bytes = b"") -> http.Response:
        path = "/" + _quote(self.name) + ("/" + urllib.parse.quote(key, safe="/-_.~") if key else "")
        params = params or {}
        headers = sign(method, self.host, path, params, {}, hashlib.sha256(body).hexdigest(), self.access,
                       self.secret, self.clock())
        headers.pop("Host")   # the client sets it from the URL
        query = ("?" + canonical_query(params)) if params else ""
        return self.send(method, self.host, path + query, headers=headers, body=body or None, timeout=60)

    def _fail(self, what: str, resp: http.Response) -> S3Error:
        return S3Error(f"{what} on s3://{self.name}: HTTP {resp.status} {resp.text(200)}", resp.status)

    def put(self, key: str, data: bytes) -> None:
        resp = self._call("PUT", key, body=data)
        if resp.status not in (200, 201):
            raise self._fail(f"PUT {key}", resp)

    def get(self, key: str) -> Optional[bytes]:
        resp = self._call("GET", key)
        if resp.status == 404:
            return None
        if resp.status != 200:
            raise self._fail(f"GET {key}", resp)
        return resp.body

    def delete(self, key: str) -> None:
        resp = self._call("DELETE", key)
        if resp.status not in (200, 204, 404):
            raise self._fail(f"DELETE {key}", resp)

    def keys(self, prefix: str = "") -> list:
        """Object keys under `prefix` (one page of up to 1000; the lab's bucket holds a handful)."""
        resp = self._call("GET", params={"list-type": "2", "prefix": prefix})
        if resp.status != 200:
            raise self._fail(f"list {prefix!r}", resp)
        return re.findall(r"<Key>([^<]*)</Key>", resp.body.decode("utf-8", errors="replace"))


def assume_role(send: Callable, access: str, secret: str, role_arn: str = "arn:aws:iam::000000000000:role/LakekeeperVendedRole",
                host: str = "s3.lab.test", clock: Callable = lambda: datetime.datetime.now(datetime.timezone.utc)
                ) -> http.Response:
    """STS AssumeRole against SeaweedFS's S3 endpoint as one identity (form POST, SigV4 service `sts`)."""
    body = urllib.parse.urlencode({"Action": "AssumeRole", "Version": "2011-06-15", "RoleArn": role_arn,
                                   "RoleSessionName": "lab-check", "DurationSeconds": "900"}).encode()
    headers = sign("POST", host, "/", {}, {"Content-Type": "application/x-www-form-urlencoded"},
                   hashlib.sha256(body).hexdigest(), access, secret, clock(), service="sts")
    headers.pop("Host")
    return send("POST", host, "/", headers=headers, body=body, timeout=60)
