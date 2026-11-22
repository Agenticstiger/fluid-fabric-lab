# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Every credential the lab generates, and the Secrets that carry them.

OpenBao is the source of truth: `lab up` reads `kv/data/lab/credentials`,
generates only what is missing, writes the set back, and then renders the
Kubernetes Secrets below from it. A re-run therefore keeps every password.
Nothing here is written to disk or printed; Secrets reach the cluster on
kubectl's stdin.

Where each credential goes (Secret, namespace, key):
- PostgreSQL roles (CNPG managed roles): `lab-pg-<role>` (basic-auth) in
  lab-db, plus a copy in the namespace of the service that logs in with it;
  OpenFGA reads a URI instead (`lab-pg-openfga-uri`).
- OpenFGA pre-shared key: `openfga-preshared-keys` in openfga (key `keys`,
  the chart's name) and in lakekeeper (key `lakekeeper`).
- Lakekeeper's secret-store encryption key: `lakekeeper-pg-encryption` in
  lakekeeper (its warehouse credentials are encrypted with it in Postgres).
- Keycloak client secrets: `keycloak-realm-clients` in keycloak (realm
  import placeholders); Trino's own copy as `trino-lakekeeper-client`.
- Trino's server secrets: `trino-server-secrets` in trino (internal-communication
  shared secret, which JWT authentication requires, and the keystore password
  cert-manager seals the coordinator's certificate with).
- The pipeline identity: `fluid-pipeline-client` in spark (client id and secret
  for `lab-dbt`), beside `spark-lakekeeper-client`.
- SeaweedFS: `seaweedfs-s3-config`, the identities plus STS signing key,
  role and policy (one JSON file read by both -config and -iam.config).
- Airflow (orchestration tier, namespace airflow): `airflow-metadata` (the
  metadata DB URI on the CNPG role `airflow`), `airflow-fernet-key`,
  `airflow-api-secret-key`, `airflow-jwt-secret`, `airflow-simple-auth` (the
  lab's one API/UI login: no default admin user), and `airflow-s3-connection`
  (Airflow connection `lab_s3` as JSON: the SeaweedFS identity `airflow`, read
  and list on bucket lab-airflow only). The identity `airflow-publish`, which
  may also write there, is for whoever uploads DAGs (CI; `lab check`): its key
  is in OpenBao and in no pod.
- The spark tier's two identities, both limited to bucket `lab-spark-artifacts`
  (the warehouse bucket is closed to them) and both Secrets in spark with
  AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
  `spark-artifacts-s3` (`s3.spark.*`), for fluid-host: forge uploads the runner
  and job.json there; `spark-build-s3` (`s3.sparkbuild.*`), for the Spark build
  pods: read `runs*`, read and write `_checkpoints*` (forge's prefix for a local stream).
  SeaweedFS identities scope prefixes with wildcard actions (`Write:bucket/prefix*`;
  `CanDo` in weed/s3api/auth_credentials.go), so the build key cannot overwrite
  what forge uploaded.
"""

from __future__ import annotations

import base64
import json
import secrets
import string
from urllib.parse import quote

PG_ROLES = ("lakekeeper", "openfga", "keycloak", "airflow", "tofu")
CLIENTS = ("lakekeeper", "lab-admin", "trino", "spark", "lab-probe", "fluid-pipeline")
# Which namespace holds a copy of each role's Secret (besides lab-db).
PG_CONSUMERS = {"lakekeeper": "lakekeeper", "keycloak": "keycloak", "airflow": "airflow"}
PG_HOST = "lab-pg-rw.lab-db.svc"
WAREHOUSE_BUCKET = "lake"
AIRFLOW_BUCKET = "lab-airflow"
AIRFLOW_ADMIN = "lab-admin"   # the one login of Airflow's API and UI (SimpleAuthManager)
ARTIFACTS_BUCKET = "lab-spark-artifacts"
STS_ROLE = "LakekeeperVendedRole"
STS_ACCOUNT = "000000000000"
STS_ROLE_ARN = f"arn:aws:iam::{STS_ACCOUNT}:role/{STS_ROLE}"
# SeaweedFS names a static identity's principal `arn:aws:iam::<account>:user/<identity name>`
# (weed/s3api/s3api_sts.go callerPrincipalArn) and matches a trust policy's `Principal.AWS` against it.
LAKEKEEPER_PRINCIPAL = f"arn:aws:iam::{STS_ACCOUNT}:user/lakekeeper"
_ALNUM = string.ascii_uppercase + string.digits


def _password(n: int = 32) -> str:
    return secrets.token_urlsafe(n)


def _access_key() -> str:
    return "LAB" + "".join(secrets.choice(_ALNUM) for _ in range(17))


def generate(existing: dict) -> tuple:
    """Fill in every missing credential. Returns (credentials, changed)."""
    creds = dict(existing or {})
    wanted = {f"pg.{role}": _password for role in PG_ROLES}
    wanted.update({f"client.{c}": _password for c in CLIENTS})
    wanted.update({
        "openfga.preshared": lambda: _password(32),
        "trino.shared_secret": lambda: _password(32),
        "trino.keystore_password": lambda: _password(24),
        "lakekeeper.pg_encryption_key": lambda: _password(40),
        "s3.admin.access": _access_key, "s3.admin.secret": lambda: _password(30),
        "s3.lakekeeper.access": _access_key, "s3.lakekeeper.secret": lambda: _password(30),
        "s3.airflow.access": _access_key, "s3.airflow.secret": lambda: _password(30),
        "s3.airflow_publish.access": _access_key, "s3.airflow_publish.secret": lambda: _password(30),
        # A Fernet key is 32 random bytes, url-safe base64 (cryptography.fernet.Fernet.generate_key).
        "airflow.fernet_key": lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "airflow.api_secret_key": lambda: _password(32), "airflow.jwt_secret": lambda: _password(48),
        "airflow.admin_password": lambda: _password(24),
        "s3.spark.access": _access_key, "s3.spark.secret": lambda: _password(30),
        "s3.sparkbuild.access": _access_key, "s3.sparkbuild.secret": lambda: _password(30),
        "s3.sts.signing_key": lambda: base64.b64encode(secrets.token_bytes(32)).decode(),
    })
    changed = False
    for key, make in wanted.items():
        if not creds.get(key):
            creds[key] = make()
            changed = True
    return creds, changed


def seaweedfs_config(creds: dict) -> dict:
    """The S3 identities plus the advanced IAM (STS) config, in one document.

    Shape from Lakekeeper's .github/seaweedfs-iam.json at v0.14.0
    (Apache-2.0). Unlike that CI file, the role's policy reaches only the
    warehouse bucket, and every key is generated per lab.
    """
    lake = [f"arn:aws:s3:::{WAREHOUSE_BUCKET}", f"arn:aws:s3:::{WAREHOUSE_BUCKET}/*"]
    bucket = AIRFLOW_BUCKET
    return {
        "identities": [
            {"name": "lab-admin",
             "credentials": [{"accessKey": creds["s3.admin.access"], "secretKey": creds["s3.admin.secret"]}],
             "actions": ["Admin", "Read", "List", "Tagging", "Write"]},
            {"name": "lakekeeper",
             "credentials": [{"accessKey": creds["s3.lakekeeper.access"],
                              "secretKey": creds["s3.lakekeeper.secret"]}],
             "actions": [f"Read:{WAREHOUSE_BUCKET}", f"List:{WAREHOUSE_BUCKET}",
                         f"Tagging:{WAREHOUSE_BUCKET}", f"Write:{WAREHOUSE_BUCKET}"]},
            # Airflow's DAG bundles read s3://lab-airflow and nothing else.
            {"name": "airflow",
             "credentials": [{"accessKey": creds["s3.airflow.access"], "secretKey": creds["s3.airflow.secret"]}],
             "actions": [f"Read:{bucket}", f"List:{bucket}"]},
            # Whoever publishes DAGs and project files (CI, `lab check`): the same bucket, plus write and delete.
            {"name": "airflow-publish",
             "credentials": [{"accessKey": creds["s3.airflow_publish.access"],
                              "secretKey": creds["s3.airflow_publish.secret"]}],
             "actions": [f"Read:{bucket}", f"List:{bucket}", f"Write:{bucket}", f"Tagging:{bucket}"]},
            {"name": "spark-artifacts",
             "credentials": [{"accessKey": creds["s3.spark.access"], "secretKey": creds["s3.spark.secret"]}],
             "actions": [f"Read:{ARTIFACTS_BUCKET}", f"List:{ARTIFACTS_BUCKET}",
                         f"Tagging:{ARTIFACTS_BUCKET}", f"Write:{ARTIFACTS_BUCKET}"]},
            {"name": "spark-build",
             "credentials": [{"accessKey": creds["s3.sparkbuild.access"],
                              "secretKey": creds["s3.sparkbuild.secret"]}],
             # Wildcards are what SeaweedFS needs for an object prefix: a plain action matches the bucket only.
             "actions": [f"Read:{ARTIFACTS_BUCKET}/runs*", f"List:{ARTIFACTS_BUCKET}/runs*",
                         # forge keeps a local stream's checkpoint under _checkpoints/<product>/ (0.36.0 refuses
                         # any other prefix), in the bucket the contract names.
                         f"Read:{ARTIFACTS_BUCKET}/_checkpoints*", f"List:{ARTIFACTS_BUCKET}/_checkpoints*",
                         f"Write:{ARTIFACTS_BUCKET}/_checkpoints*"]},
        ],
        "sts": {"tokenDuration": "1h", "maxSessionLength": "12h", "issuer": "fluid-fabric-lab-sts",
                "signingKey": creds["s3.sts.signing_key"]},
        "roles": [{
            "roleName": STS_ROLE, "roleArn": STS_ROLE_ARN,
            "trustPolicy": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": LAKEKEEPER_PRINCIPAL}, "Action": "sts:AssumeRole"}]},
            "attachedPolicies": ["LakeWarehouseAccess"],
        }],
        "policies": [{"name": "LakeWarehouseAccess", "document": {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:*"], "Resource": lake}]}}],
    }


def secrets_for(creds: dict, namespaces: set) -> list:
    """Kubernetes Secret manifests, limited to namespaces that exist in this tier."""
    from labctl.kube import secret

    out = []

    def add(name, ns, data, kind="Opaque"):
        if ns in namespaces:
            out.append(secret(name, ns, data, kind=kind))

    for role in PG_ROLES:
        data = {"username": role, "password": creds[f"pg.{role}"]}
        add(f"lab-pg-{role}", "lab-db", data, "kubernetes.io/basic-auth")
        if role in PG_CONSUMERS:
            add(f"lab-pg-{role}", PG_CONSUMERS[role], data, "kubernetes.io/basic-auth")
    add("lab-pg-openfga-uri", "openfga",
        {"uri": f"postgres://openfga:{creds['pg.openfga']}@{PG_HOST}:5432/openfga?sslmode=require"})
    add("openfga-preshared-keys", "openfga", {"keys": creds["openfga.preshared"]})
    add("openfga-preshared-keys", "lakekeeper", {"lakekeeper": creds["openfga.preshared"]})
    add("lakekeeper-pg-encryption", "lakekeeper", {"encryptionKey": creds["lakekeeper.pg_encryption_key"]})
    add("keycloak-realm-clients", "keycloak", {c: creds[f"client.{c}"] for c in CLIENTS})
    add("trino-lakekeeper-client", "trino",
        {"LAKEKEEPER_CLIENT_ID": "trino", "LAKEKEEPER_CLIENT_SECRET": creds["client.trino"]})
    add("spark-lakekeeper-client", "spark",
        {"LAKEKEEPER_CLIENT_ID": "spark", "LAKEKEEPER_CLIENT_SECRET": creds["client.spark"]})
    add("spark-artifacts-s3", "spark",
        {"AWS_ACCESS_KEY_ID": creds["s3.spark.access"], "AWS_SECRET_ACCESS_KEY": creds["s3.spark.secret"]})
    add("spark-build-s3", "spark",
        {"AWS_ACCESS_KEY_ID": creds["s3.sparkbuild.access"],
         "AWS_SECRET_ACCESS_KEY": creds["s3.sparkbuild.secret"]})
    add("trino-server-secrets", "trino",
        {"TRINO_SHARED_SECRET": creds["trino.shared_secret"],
         "TRINO_KEYSTORE_PASSWORD": creds["trino.keystore_password"]})
    add("fluid-pipeline-client", "spark",
        {"FLUID_CLIENT_ID": "fluid-pipeline", "FLUID_CLIENT_SECRET": creds["client.fluid-pipeline"]})
    add("seaweedfs-s3-config", "seaweedfs", {"seaweedfs_s3_config": json.dumps(seaweedfs_config(creds))})
    for name, data in airflow_secrets(creds).items():
        add(name, "airflow", data)
    return out


def airflow_secrets(creds: dict) -> dict:
    """The Secrets of namespace airflow, by name. Key names are the chart's (Airflow chart 1.22.0)."""
    return {
        # The metadata DB: the CNPG role `airflow`, database `airflow`. The password is URL-quoted.
        "airflow-metadata": {"connection": f"postgresql://airflow:{quote(creds['pg.airflow'], safe='')}@{PG_HOST}:5432"
                                           "/airflow?sslmode=require"},
        "airflow-fernet-key": {"fernet-key": creds["airflow.fernet_key"]},
        "airflow-api-secret-key": {"api-secret-key": creds["airflow.api_secret_key"]},
        "airflow-jwt-secret": {"jwt-secret": creds["airflow.jwt_secret"]},
        # Airflow's SimpleAuthManager reads {"user": "password"} from this file; the user's role is in values.
        "airflow-simple-auth": {"passwords.json": json.dumps({AIRFLOW_ADMIN: creds["airflow.admin_password"]})},
        # Airflow connection `lab_s3` (JSON form, AIRFLOW_CONN_<ID>): SeaweedFS through the Gateway name, path-style,
        # checked against the lab CA that trust-manager mounts at /etc/lab-ca.
        "airflow-s3-connection": {"AIRFLOW_CONN_LAB_S3": json.dumps({
            "conn_type": "aws", "login": creds["s3.airflow.access"], "password": creds["s3.airflow.secret"],
            "extra": {"region_name": "us-east-1", "endpoint_url": "https://s3.lab.test",
                      "verify": "/etc/lab-ca/ca.crt", "config_kwargs": {"s3": {"addressing_style": "path"}}}})},
    }
