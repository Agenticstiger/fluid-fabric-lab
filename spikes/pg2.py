# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
# PG-2: PG-1 inside forge's secure_duckdb_connect, the ATTACH in before_lock; REST and S3 reads must
# work after the configuration is locked, and the sandbox itself must refuse three things.
import json, os, sys
import duckdb
from fluid_build.providers._duckdb_sandbox import DuckDBAllowlist, secure_duckdb_connect
cid, sec = os.environ["LAKEKEEPER_CLIENT_ID"], os.environ["LAKEKEEPER_CLIENT_SECRET"]
out = {}
def error(exc):
    # DuckDB's parser and binder errors quote the statement, and CREATE SECRET's holds the secret.
    return f"{type(exc).__name__}: {str(exc).replace(sec, '<redacted>')[:300]}"
def attach(con, ca=True):
    if ca:
        # forge pins ca_cert_file only after before_lock; the ATTACH below fetches its token now.
        con.execute("SET ca_cert_file = '/etc/lab-ca/ca.crt'")
    con.execute(f"""CREATE SECRET lk (TYPE iceberg, CLIENT_ID '{cid}', CLIENT_SECRET '{sec}',
      OAUTH2_SERVER_URI 'https://keycloak.lab.test/realms/fluid/protocol/openid-connect/token', OAUTH2_SCOPE 'lakekeeper')""")
    con.execute("ATTACH 'sales-wh' AS lk (TYPE iceberg, SECRET lk, ENDPOINT 'https://lakekeeper.lab.test/catalog')")
allow = DuckDBAllowlist.none().with_remote("s3://lake/sales-wh/", "https://lakekeeper.lab.test/", "https://keycloak.lab.test/")
def connect(ca=True):
    return secure_duckdb_connect(":memory:", allow=allow, extensions=("httpfs", "iceberg"),
                                 before_lock=lambda con: attach(con, ca))
# The forge finding, shown: the same ATTACH without its own SET ca_cert_file, in before_lock.
try:
    connect(ca=False).close()
    out["before_lock_without_ca"] = "attached"
except Exception as exc:
    out["before_lock_without_ca"] = error(exc)
try:
    con = connect()
    out["locked"] = {k: con.execute(f"SELECT current_setting('{k}')").fetchone()[0]
                     for k in ("lock_configuration", "enable_external_access", "allowed_directories")}
    out["select"] = [list(map(str, r)) for r in con.execute("SELECT event_id FROM lk.raw.events ORDER BY 1").fetchall()]
except Exception as exc:
    # Without a locked connection that still reads, the refusals below would prove nothing.
    out["error"] = error(exc)
    print("PG2 " + json.dumps(out)); sys.exit(1)
# Only the sandbox's own refusal counts: a path outside allowed_directories is a PermissionException,
# and a locked option an InvalidInputException that says so. Any other error proves nothing.
for probe, sql, refusal in (("local_file", "SELECT * FROM read_csv('/etc/passwd')", duckdb.PermissionException),
                            ("other_bucket", "SELECT * FROM read_parquet('s3://other/x.parquet')",
                             duckdb.PermissionException),
                            ("unlock", "SET lock_configuration = false", duckdb.InvalidInputException)):
    try:
        con.execute(sql).fetchall()
        out[probe] = {"refused": False}
    except Exception as exc:
        out[probe] = {"refused": isinstance(exc, refusal) and (probe != "unlock" or "locked" in str(exc)),
                      "error": error(exc)}
print("PG2 " + json.dumps(out))
