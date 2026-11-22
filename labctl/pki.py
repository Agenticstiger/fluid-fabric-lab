# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""The lab CA, created on the host before anything else starts.

OpenBao runs beside the cluster and needs a TLS certificate before
cert-manager exists, so the CA cannot come from cert-manager (the open
question in platform/keys/openbao/README.md). `lab up` therefore creates the
CA with `openssl` in the lab's state directory, signs OpenBao's certificate
with it, and imports the CA key pair into the cluster as Secret
`cert-manager/lab-ca`, where the `lab-ca` ClusterIssuer signs every in-cluster
endpoint certificate and trust-manager copies the CA certificate (never the
key) into every namespace.

ECDSA P-256 keys; the CA is constrained to one level (pathlen 0). Keys are
written 0600 inside the 0700 state directory, so no other user can read them.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Iterable

from labctl import proc
from labctl.state import ensure_dir

CA_DAYS = 825
LEAF_DAYS = 397


class PKIError(Exception):
    pass


def _run(runner: proc.Runner, argv: list, what: str) -> None:
    res = runner(argv, timeout=60)
    if res.missing:
        raise PKIError("openssl is not on PATH; it creates the lab CA (install it from your OS packages)")
    if not res.ok:
        raise PKIError(f"openssl failed to {what}: {res.output_tail(500).strip()}")


def _new_key(runner: proc.Runner, path: Path) -> None:
    _run(runner, ["openssl", "genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256",
                  "-out", str(path)], "create a key")
    os.chmod(path, 0o600)


def ca_paths(ca_dir: Path) -> tuple:
    return ca_dir / "ca.crt", ca_dir / "ca.key"


def ensure_ca(runner: proc.Runner, ca_dir: Path, lab_name: str) -> tuple:
    """Create the CA once; reuse it afterwards. Returns (crt, key) paths."""
    ensure_dir(ca_dir)
    crt, key = ca_paths(ca_dir)
    if crt.is_file() and key.is_file():
        return crt, key
    _new_key(runner, key)
    _run(runner, [
        "openssl", "req", "-x509", "-new", "-key", str(key), "-sha256", "-days", str(CA_DAYS),
        "-subj", f"/O=fluid-fabric-lab/CN=fluid-fabric-lab {lab_name} CA",
        "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-addext", "subjectKeyIdentifier=hash",
        "-out", str(crt),
    ], "create the lab CA")
    return crt, key


def issue(runner: proc.Runner, ca_dir: Path, out_dir: Path, common_name: str,
          dns_names: Iterable[str] = (), ips: Iterable[str] = ()) -> tuple:
    """A server certificate for `common_name` signed by the lab CA. Returns (crt, key)."""
    ca_crt, ca_key = ca_paths(ca_dir)
    if not (ca_crt.is_file() and ca_key.is_file()):
        raise PKIError(f"no lab CA in {ca_dir}; ensure_ca first")
    ensure_dir(out_dir)
    key, csr, crt, ext = (out_dir / n for n in ("tls.key", "tls.csr", "tls.crt", "tls.ext"))
    sans = [f"DNS:{n}" for n in dns_names] + [f"IP:{ip}" for ip in ips]
    if not sans:
        raise PKIError(f"certificate {common_name!r} needs at least one subject alternative name")
    ext.write_text(
        "[v3]\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=serverAuth\n"
        "authorityKeyIdentifier=keyid\n"
        f"subjectAltName={','.join(sans)}\n",
        encoding="utf-8",
    )
    _new_key(runner, key)
    _run(runner, ["openssl", "req", "-new", "-key", str(key), "-subj", f"/CN={common_name}", "-out", str(csr)],
         f"create a request for {common_name}")
    _run(runner, ["openssl", "x509", "-req", "-in", str(csr), "-CA", str(ca_crt), "-CAkey", str(ca_key),
                  "-set_serial", "0x" + secrets.token_hex(16), "-days", str(LEAF_DAYS), "-sha256",
                  "-extfile", str(ext), "-extensions", "v3", "-out", str(crt)],
         f"sign the certificate for {common_name}")
    csr.unlink(missing_ok=True)
    ext.unlink(missing_ok=True)
    return crt, key
