# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
# PG-1: DuckDB 1.5.6 iceberg ATTACH to Lakekeeper over OAuth2 with plain duckdb.connect;
# SELECT and INSERT with vended credentials and no AWS_* key.
import json, os, sys, time
import duckdb
assert not any(k.startswith("AWS_") for k in os.environ), "an AWS_* variable is set"
cid, sec = os.environ["LAKEKEEPER_CLIENT_ID"], os.environ["LAKEKEEPER_CLIENT_SECRET"]
out = {"duckdb": duckdb.__version__}
con = duckdb.connect()
def step(name, sql):
    t = time.monotonic()
    try:
        res = con.execute(sql).fetchall()
        out[name] = {"ok": True, "rows": [list(map(str, r)) for r in res][:5], "s": round(time.monotonic() - t, 2)}
    except Exception as exc:
        # DuckDB's parser and binder errors quote the statement, and CREATE SECRET's holds the secret.
        out[name] = {"ok": False, "error": f"{type(exc).__name__}: {str(exc).replace(sec, '<redacted>')[:400]}"}
        print("PG1 " + json.dumps(out)); sys.exit(1)
step("install", "INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs")
step("ca", "SET ca_cert_file = '/etc/lab-ca/ca.crt'")
step("secret", f"""CREATE SECRET lk (TYPE iceberg, CLIENT_ID '{cid}', CLIENT_SECRET '{sec}',
  OAUTH2_SERVER_URI 'https://keycloak.lab.test/realms/fluid/protocol/openid-connect/token', OAUTH2_SCOPE 'lakekeeper')""")
step("attach", "ATTACH 'sales-wh' AS lk (TYPE iceberg, SECRET lk, ENDPOINT 'https://lakekeeper.lab.test/catalog')")
step("select", "SELECT event_id, user_id, CAST(event_time AS VARCHAR) FROM lk.raw.events ORDER BY event_id")
step("insert", "INSERT INTO lk.raw.events VALUES ('d1', 'duckdb', TIMESTAMP '2026-11-18 21:00:00')")
step("select_after", "SELECT count(*) FROM lk.raw.events")
step("secrets", "SELECT name, type, provider FROM duckdb_secrets()")
print("PG1 " + json.dumps(out))
