# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 selftest: every gate catches its planted cases, passes its good ones,
and passes on the real repository.

  python3 tools/l0/selftest [--only GATE[,GATE]] [--fixtures-only | --repo-only] [-v]

It lives in a directory (tools/l0/selftest/) so `lab l0` does not run it as a
gate; it runs the gates.

How a case runs: the fixture directory under tests/fixtures/ is COPIED to a
fresh temporary directory first (so the repository's own REUSE.toml,
.gitleaks.toml allowlist and brand-fence path allowlist for tests/fixtures/
do not apply to it), optionally prepared (chart directories packed into .tgz
archives, LICENSES/ added, a chart byte flipped), then the gate is run as a
subprocess with every proxy variable pointed at a closed port. A case passes
when the exit code AND a required output line both match. The selftest
also fails if a gate in tools/l0/ has no planted (failing) case or no
real-repository case. Exit 0 only when every case matches.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
L0 = Path(__file__).resolve().parent.parent
REPO = L0.parent.parent
FX = REPO / "tests" / "fixtures"


def gates() -> tuple:
    """(gate names, inventory problems): the same discovery and registry rules
    as `lab l0` (labctl/l0.py, reused rather than copied)."""
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from labctl import l0  # noqa: E402

    found, problems = l0.inventory(L0)
    return [g.name for g in found], problems


# ------------------------------------------------------------- preparation


def pack_charts(root: Path) -> None:
    """Turn every platform/charts/<dir> with a Chart.yaml into <name>-<version>.tgz (helm layout)."""
    charts = root / "platform" / "charts"
    for d in sorted(p for p in charts.iterdir() if p.is_dir() and (p / "Chart.yaml").is_file()):
        text = (d / "Chart.yaml").read_text()
        name = re.search(r"^name:\s*(\S+)", text, re.M).group(1)
        ver = re.search(r"^version:\s*(\S+)", text, re.M).group(1)
        with tarfile.open(charts / f"{name}-{ver}.tgz", "w:gz") as tf:
            tf.add(d, arcname=name)
        shutil.rmtree(d)


def pack_nested(root: Path) -> None:
    """Pack each platform/charts/<chart>/charts/<sub>/ into <sub>-<version>.tgz in place (a vendored subchart archive)."""
    for sub in sorted((root / "platform" / "charts").glob("*/charts/*/Chart.yaml")):
        d = sub.parent
        text = sub.read_text()
        name = re.search(r"^name:\s*(\S+)", text, re.M).group(1)
        ver = re.search(r"^version:\s*(\S+)", text, re.M).group(1)
        with tarfile.open(d.parent / f"{name}-{ver}.tgz", "w:gz") as tf:
            tf.add(d, arcname=name)
        shutil.rmtree(d)


def good_chart(root: Path) -> None:
    """Add the allowed Apache-2.0 chart (licence/apache-good) to the fixture's
    chart cache, packed, with its REUSE annotation: the licence gate fails a
    tree with no chart cache, so cases about something else need one."""
    charts = root / "platform" / "charts"
    charts.mkdir(parents=True, exist_ok=True)
    src = FX / "licence" / "apache-good" / "platform" / "charts" / "good-chart"
    with tarfile.open(charts / "good-chart-0.1.0.tgz", "w:gz") as tf:
        tf.add(src, arcname="good-chart")
    reuse = root / "REUSE.toml"
    block = ('\n[[annotations]]\npath = ["platform/charts/good-chart-0.1.0.tgz"]\n'
             'SPDX-FileCopyrightText = "2026 Agentics Transformation Limited"\n'
             'SPDX-License-Identifier = "Apache-2.0"\n')
    if reuse.is_file():
        reuse.write_text(reuse.read_text() + block)
    else:
        reuse.write_text("version = 1\n" + block)


def mini_lock(root: Path) -> None:
    """A minimal versions.lock.yaml when the fixture has none (the licence gate requires one)."""
    lock = root / "versions.lock.yaml"
    if not lock.is_file():
        lock.write_text("lock_version: 1\ncharts: []\nimages: []\n")


def add_licenses(root: Path) -> None:
    """Fixture trees are Apache-2.0 only; add that licence text (an unused one would fail REUSE)."""
    (root / "LICENSES").mkdir(exist_ok=True)
    shutil.copy2(REPO / "LICENSES" / "Apache-2.0.txt", root / "LICENSES" / "Apache-2.0.txt")


def repo_subset(root: Path) -> None:
    for n in ("platform", "tiers.yaml", "versions.lock.yaml"):
        s = REPO / n
        (shutil.copytree(s, root / n, symlinks=True) if s.is_dir() else shutil.copy2(s, root / n))


def empty_root(root: Path) -> None:
    """Remove every file of the copied fixture: the gate is run on an empty tree."""
    shutil.rmtree(root)
    root.mkdir()


def pycache(root: Path) -> None:
    """Rename fixture directories called PYCACHE to __pycache__ (the real tree
    keeps no __pycache__ directory: .gitignore drops them and the brand fence
    fails on anything but .pyc inside one)."""
    for d in sorted(root.rglob("PYCACHE"), key=lambda p: -len(p.parts)):
        d.rename(d.with_name("__pycache__"))


def repo_overlay(root: Path) -> None:
    """A copy of the repository's platform/, tiers.yaml and versions.lock.yaml
    with the fixture's files laid over it."""
    stash = root.parent / "fixture"
    root.rename(stash)
    root.mkdir()
    repo_subset(root)
    shutil.copytree(stash, root, dirs_exist_ok=True)


def uncached_chart(root: Path) -> None:
    """Unpack the cached Lakekeeper chart to products/lk/ and point the release at it."""
    archive = sorted((root / "platform/charts").glob("lakekeeper-*.tgz"))[0]
    dest = root / "products" / "lk"
    dest.mkdir(parents=True)
    with tarfile.open(archive) as tf:
        for m in tf.getmembers():
            if m.isfile() and not m.name.startswith("/") and ".." not in Path(m.name).parts:
                (dest / m.name).parent.mkdir(parents=True, exist_ok=True)
                (dest / m.name).write_bytes(tf.extractfile(m).read())
    hf = root / "platform/helmfile.yaml.gotmpl"
    old = '  - name: lakekeeper\n    namespace: lakekeeper\n    chart: ./{{ trimPrefix "platform/" $c.file }}'
    text = hf.read_text()
    if old not in text:
        raise SystemExit("selftest: lakekeeper release block not found in the Helmfile")
    hf.write_text(text.replace(old, '  - name: lakekeeper\n    namespace: lakekeeper\n    chart: ../products/lk/lakekeeper'))


def inject_helmfile(root: Path) -> None:
    """Insert the fixture's inject.gotmpl into the Helmfile, before kubeVersion."""
    hf = root / "platform/helmfile.yaml.gotmpl"
    text = hf.read_text()
    hf.write_text(text.replace("\nkubeVersion:", "\n" + (root / "inject.gotmpl").read_text() + "kubeVersion:", 1))


def drop_trino(root: Path) -> None:
    """Delete trino from the core tier in tiers.yaml (only there)."""
    tiers = root / "tiers.yaml"
    text = tiers.read_text()
    core = text.index("\n  core:")
    i = text.index("      - trino\n", core)
    tiers.write_text(text[:i] + text[i + len("      - trino\n"):])


def tamper_chart(root: Path) -> None:
    repo_subset(root)
    victim = sorted((root / "platform/charts").glob("trino-*.tgz"))[0]
    with open(victim, "ab") as fh:
        fh.write(b"\0")


# ------------------------------------------------------------------ cases
# (gate, case id, fixture dir relative to tests/fixtures or None, args,
#  expected exit, regex that must appear in the output, preparation steps)
# Placeholders in args: {root} = the prepared copy, {fx} = tests/fixtures (read only).

def lic(case, exit_, rx, pack=True, prep=None):
    return ("licence", f"licence/{case}", f"licence/{case}", ["--root", "{root}"], exit_, rx,
            (["pack"] if pack else []) + (prep or []) + ["minilock"])


CASES = [
    # render
    ("render", "render/remote-chart", None,
     ["--tier", "core", "--helmfile", "{fx}/render/remote-chart/helmfile.yaml.gotmpl"], 1,
     r"FAIL .*tier core: helmfile-template", []),
    # The OCI pull is attempted and stopped by the poisoned proxy: proof that no network is reachable.
    ("render", "render/remote-oci-chart", None,
     ["--tier", "core", "--helmfile", "{fx}/render/remote-oci-chart/helmfile.yaml.gotmpl"], 1,
     r"proxyconnect tcp: dial tcp 127\.0\.0\.1:9", []),
    ("render", "render/tampered-cache", None, ["--root", "{root}", "--tier", "core"], 1,
     r"FAIL .*tier core: chart-cache", ["tamper"]),
    ("render", "render/dotfile-in-cache", "render/dotfile-in-cache", ["--root", "{root}", "--tier", "core"], 1,
     r"FAIL .*tier core: chart-cache.*keycloak-operator/\.helmignore: present but not listed", ["repo-overlay"]),
    ("render", "render/uncached-chart", "render/uncached-chart", ["--root", "{root}", "--tier", "core"], 1,
     r"FAIL .*tier core: chart-source.*release lakekeeper: chart products/lk/lakekeeper is not a chart",
     ["repo-overlay", "uncached"]),
    ("render", "render/exec-in-helmfile", "render/exec-in-helmfile", ["--root", "{root}", "--tier", "core"], 1,
     r"FAIL .*tier core: helmfile-static.*template function `exec` can reach the network",
     ["repo-overlay", "inject"]),
    ("render", "render/tier-drops-release", "render/tier-drops-release", ["--root", "{root}", "--tier", "core"], 1,
     r"FAIL .*tier core: tier-membership.*release trino: versions\.lock\.yaml puts chart trino in tier core",
     ["repo-overlay", "droptrino"]),
    # schema
    ("schema", "schema/good", None, ["--manifests", "{fx}/schema/good"], 0, r"PASS .*valid against vendored", []),
    ("schema", "schema/unknown-field", None, ["--manifests", "{fx}/schema/unknown-field"], 1,
     r"FAIL .*Deployment/typo.*additional properties 'replica'", []),
    ("schema", "schema/no-schema-kind", None, ["--manifests", "{fx}/schema/no-schema-kind"], 1,
     r"FAIL .*Widget/w.*could not find schema", []),
    # licence
    lic("busl", 1, r"FAIL .*acme-engine.*BUSL-1\.1"),
    lic("sspl", 1, r"FAIL .*docstore.*SSPL-1\.0"),
    lic("agpl", 1, r"FAIL .*objstore-console.*AGPL-3\.0"),
    lic("unlicensed", 1, r"FAIL .*no-licence.*unlicensed"),
    lic("ilum-eula", 1, r"FAIL .*spark-suite.*LicenseRef-EULA"),  # brand-fence: allow
    lic("vendored-osl", 1, r"FAIL .*sdp-operator.*OSL-3\.0 \(vendored\)", pack=False),
    ("licence", "licence/vendored-osl (REUSE header)", "licence/vendored-osl", ["--root", "{root}"], 1,
     r"FAIL .*sdp-overlay/patch\.yaml: denied: OSL-3\.0 \(vendored\)", []),
    lic("runtime-osl", 0, r"NOTE .*recorded: image example-operator.*OSL-3\.0: runtime reference only", pack=False,
        prep=["goodchart"]),
    lic("runtime-osl-no-obligations", 1, r"FAIL .*example-operator.*OSL-3\.0 \(runtime reference without", pack=False),
    lic("ubi-eula", 0, r"NOTE .*recorded: platform/images/base/EULA-ubi\.txt: LicenseRef-UBI-EULA", pack=False,
        prep=["goodchart"]),
    lic("apache-good", 0, r"PASS .*1/1 vendored chart"),
    # each case pins one way a denied licence text could be hidden
    lic("renamed-text", 1, r"FAIL .*docs-chart-0\.1\.0\.tgz > docs-chart/legal/agpl-3\.0\.txt: names denied licence "
        r"text: AGPL-3\.0"),
    lic("header-lies", 1, r"FAIL platform/components/vendored/terms\.txt: names denied licence text: AGPL-3\.0",
        pack=False, prep=["goodchart"]),
    lic("nested-archive", 1, r"FAIL .*outer-0\.1\.0\.tgz > outer/charts/evil-1\.0\.0\.tgz > evil/LICENSE: names "
        r"denied licence text: AGPL-3\.0", pack=False, prep=["packnested", "pack"]),
    lic("rider", 1, r"FAIL .*rider-chart/LICENSE: names denied licence text: AGPL-3\.0"),
    lic("chart-outside-platform", 1, r"FAIL products/lk/Chart\.yaml: a Helm chart outside platform/charts/",
        pack=False, prep=["goodchart"]),
    lic("no-cache", 1, r"FAIL no chart cache", pack=False, prep=["licenses"]),
    lic("osl-trivial", 1, r"FAIL .*example-operator: OSL-3\.0 obligations '-' are a placeholder", pack=False,
        prep=["goodchart"]),
    # denylist (manifest fixtures are scanned in place; no lock, no text)
    *[("denylist", f"denylist/{c}", None, ["--manifests", f"{{fx}}/denylist/{c}", "--lock", "none", "--no-text"],
       1, rx, []) for c, rx in [
        ("bitnamilegacy", r"FAIL .*denied \(bitnami\): docker\.io/bitnamilegacy/"),
        ("bitnami", r"FAIL .*denied \(bitnami\): docker\.io/bitnami/"),
        ("pgsty-silo", r"FAIL .*denied \(pgsty-agpl\)"),
        ("lakekeeper-plus", r"FAIL .*denied \(lakekeeper-plus\)"),
        ("redpanda", r"FAIL .*denied \(redpanda\)"),  # brand-fence: allow
        ("ilum", r"FAIL .*denied \(ilum\)"),  # brand-fence: allow
        ("minio", r"FAIL .*denied \(minio\)"),
        ("latest", r"FAIL .*:latest"),
        ("untagged", r"FAIL .*no tag and no digest"),
        ("tag-only", r"FAIL .*not pinned by sha256 digest"),
        ("env-image", r"FAIL .*env\[0\]\.value: denied \(bitnami\)"),
        ("lakekeeper-plus-mirror", r"FAIL .*containers\[0\]\.image: denied \(lakekeeper-plus\): localhost:5001/vakamo"),
        ("lakekeeper-postgresql-ghcr", r"FAIL .*denied \(bitnami-rebuild\): ghcr\.io/lakekeeper/postgresql"),
        ("percona-mongodb", r"FAIL .*denied \(mongodb-sspl\): docker\.io/percona/percona-server-mongodb"),
        ("split-org", r"FAIL .*\.data\.IMAGE_ORG: denied \(denied-org\): bitnamilegacy"),
        ("base64-secret", r"FAIL .*\.data\.image\(base64\): denied \(bitnami\): docker\.io/bitnamilegacy/kubectl"),
        ("pgsty-silo-arm64", r"FAIL .*env\[0\]\.value: denied \(pgsty-agpl\): pgsty/silo-arm64"),
        ("cr-latest", r"FAIL .*Cluster/fixture/pg .*\.spec\.imageName: image .*:latest"),
        ("cr-untagged", r"FAIL .*KafkaConnect/fixture/connect .*\.spec\.image: image .*no tag and no digest"),
        ("rc-untagged", r"FAIL .*ReplicationController/fixture/rc .*container main image quay\.io/example/tool: no tag"),
    ]],
    ("denylist", "denylist/clean", None, ["--manifests", "{fx}/denylist/clean", "--lock", "none", "--no-text"], 0,
     r"PASS .*none denied, all digest-pinned", []),
    ("denylist", "denylist/lock-used-denied", None,
     ["--manifests", "{fx}/denylist/clean", "--lock", "{fx}/denylist/lock-used-denied/versions.lock.yaml",
      "--no-text"], 1, r"FAIL .*images\[silo\].*denied \(pgsty-agpl\) but use is used", []),
    ("denylist", "denylist/lock-clean", None,
     ["--manifests", "{fx}/denylist/clean", "--lock", "{fx}/denylist/lock-clean/versions.lock.yaml", "--no-text"],
     0, r"PASS .*1 `use: denied` record", []),
    ("denylist", "denylist/helmfile-ilum-repo", "denylist/helmfile-ilum-repo",  # brand-fence: allow
     ["--root", "{root}", "--manifests", "{fx}/denylist/clean", "--lock", "none"], 1,
     r"FAIL .*helmfile\.yaml\.gotmpl:\d+: denied \(ilum\)", []),  # brand-fence: allow
    # brand fence
    ("brand_fence", "brand/planted-text", "brand/planted-text", ["--root", "{root}"], 1,
     r"FAIL .*README\.md:\d+: names 'Stackable'", []),  # brand-fence: allow
    ("brand_fence", "brand/planted-path", "brand/planted-path", ["--root", "{root}"], 1,
     r"FAIL .*path names 'redpanda'", []),  # brand-fence: allow
    ("brand_fence", "brand/unterminated-block", "brand/unterminated-block", ["--root", "{root}"], 1,
     r"FAIL .*allow-begin` without a matching", []),
    # brand-fence: allow-begin
    *[("brand_fence", f"brand/{fx} ({what})", f"brand/{fx}", ["--root", "{root}"], 1, rx, []) for fx, what, rx in [
        ("lookalike", "dash", r"FAIL README\.md:8: names 'Red-panda'"),
        ("lookalike", "dot", r"FAIL README\.md:9: names 'red\.panda'"),
        ("lookalike", "underscore", r"FAIL README\.md:10: names 'Stack_able'"),
        ("lookalike", "zero-width space", r"FAIL README\.md:11: names 'Redpanda'"),
        ("lookalike", "Cyrillic a", r"FAIL README\.md:12: names 'Redpanda'"),
        ("joined", "camel case", r"FAIL README\.md:8: names 'Stackable'"),
        ("joined", "plural", r"FAIL README\.md:9: names 'Redpanda'"),
        ("joined", "digit suffix", r"FAIL README\.md:10: names 'Confluent'"),
        ("joined", "joined word", r"FAIL README\.md:11: names 'ilum'"),
        ("joined", "joined word 2", r"FAIL README\.md:12: names 'Aquifer'"),
        ("base64", "encoded", r"FAIL README\.md:8: names 'redpanda \(base64\)'"),
        ("utf16", "UTF-16", r"FAIL README\.md:\d+: names 'Stackable' \(read as utf-16\)"),
        ("nul-byte", "NUL byte", r"FAIL README\.md:\d+: names 'Stackable' \(read as nul-stripped\)"),
    ]],
    ("brand_fence", "brand/pycache-hidden (__pycache__)", "brand/pycache-hidden", ["--root", "{root}"], 1,
     r"FAIL docs/__pycache__/notes\.md: only compiled \.pyc files", ["pycache"]),
    # brand-fence: allow-end
    ("brand_fence", "brand/allowlisted", "brand/allowlisted", ["--root", "{root}"], 0,
     r"PASS .*2 text file\(s\).*1 fully allowlisted", []),
    # reuse
    ("reuse_lint", "reuse/good", "reuse/good", ["--root", "{root}"], 0, r"PASS .*profiles/stackable/: 2 file", ["licenses"]),
    ("reuse_lint", "reuse/crd-copy", "reuse/crd-copy", ["--root", "{root}"], 1,
     r"FAIL .*trino-cluster\.yaml: contains a Kubernetes object", ["licenses"]),
    ("reuse_lint", "reuse/kustomize-patch", "reuse/kustomize-patch", ["--root", "{root}"], 1,
     r"FAIL .*kustomization\.yaml: name looks like a kustomization", ["licenses"]),
    ("reuse_lint", "reuse/no-header", "reuse/no-header", ["--root", "{root}"], 1,
     r"FAIL .*trino\.values\.yaml: needs its own header", ["licenses"]),
    ("reuse_lint", "reuse/reuse-toml-only", "reuse/reuse-toml-only", ["--root", "{root}"], 1,
     r"FAIL .*trino\.values\.yaml: needs its own header", ["licenses"]),
    ("reuse_lint", "reuse/repo-missing-info", "reuse/repo-missing-info", ["--root", "{root}"], 1,
     r"FAIL reuse lint: notes\.txt", ["licenses"]),
    # chart/app
    *[("chart_app", f"chart-app/{c}", f"chart-app/{c}", ["--root", "{root}", "--manifests", "{root}/rendered"],
       e, rx, []) for c, e, rx in [
        ("good", 0, r"PASS demo: a rendered container runs 1\.0\.0"),
        ("runs-allowed", 0, r"NOTE demo: runs 1\.1\.0 on appVersion 1\.0\.0 \(allowlisted"),
        ("appversion-drift", 1, r"FAIL demo: Chart\.yaml appVersion 1\.1\.0 != lock app_version 1\.0\.0"),
        ("runs-unallowed", 1, r"FAIL demo: runs 1\.1\.0 != appVersion 1\.0\.0 and no app_version_mismatch"),
        ("rendered-tag-drift", 1, r"FAIL .*demo:1\.1\.0@sha256:\S+: digest .* matches no lock entry"),
        ("rendered-digest-drift", 1, r"FAIL .*demo:1\.0\.0@sha256:\S+: digest .* matches no lock entry"),
        ("default-unused-rendered", 1, r"FAIL .*lock entry demo-chart-default is `use: default-unused`"),
    ]],
    ("chart_app", "chart-app/nested-archive-unrecorded", "chart-app/nested-archive-unrecorded",
     ["--root", "{root}", "--manifests", "{root}/rendered"], 1,
     r"FAIL demo: vendored subchart evil-1\.0\.0\.tgz \(evil 1\.0\.0\) is not recorded under subcharts", ["packnested"]),
    # footprint
    ("footprint", "footprint/trino-two-workers-8g", None,
     ["--tier", "core", "--values-overlay", "trino={fx}/footprint/trino-two-workers-8g/values-overlay.yaml"], 1,
     r"FAIL .*core: .*OVER TARGET", []),
    ("footprint", "footprint/trino-two-workers-8g (workers counted)", None,
     ["--tier", "core", "--values-overlay", "trino={fx}/footprint/trino-two-workers-8g/values-overlay.yaml"], 1,
     r"NOTE .*trino-worker x2 (?:[2-9]\d|1\d)\.\d+GiB", []),
    ("footprint", "footprint/stale-doc", "footprint/stale-doc", ["--root", "{root}"], 1,
     r"FAIL docs/proven\.md does not state the footprint as this gate counts it", ["repo-overlay"]),
    ("footprint", "footprint/fits", None, ["--tier", "core", "--manifests", "{fx}/footprint/fits"], 0,
     r"PASS core: \d", []),
    ("footprint", "footprint/unbounded-jvm", None, ["--tier", "core", "--manifests", "{fx}/footprint/unbounded-jvm"],
     1, r"FAIL .*MaxRAMPercentage=75 with no memory limit", []),
    ("footprint", "footprint/unmodelled-cr", None, ["--tier", "core", "--manifests", "{fx}/footprint/unmodelled-cr"],
     1, r"FAIL .*KafkaStreamsCluster.*not modelled", []),
    ("footprint", "footprint/trino-heap-hidden", None,
     ["--tier", "core", "--values-overlay", "trino={fx}/footprint/trino-heap-hidden/values-overlay.yaml"], 1,
     r"FAIL .*trino-worker/trino-worker: no memory limit", []),
    *[("footprint", f"footprint/{c}", None, ["--tier", "core", "--manifests", f"{{fx}}/footprint/{c}"], 1, rx, [])
      for c, rx in [
        ("no-limit", r"FAIL .*Deployment/fixture/nolimit/main: no memory limit"),
        ("heap-via-configmapkeyref", r"FAIL .*keyref/main: JVM heap 8192Mi .*exceeds its memory limit 1024Mi"),
        ("heap-via-envfrom", r"FAIL .*envfrom/main: JVM heap 16384Mi .*exceeds its memory limit 1024Mi"),
        ("replicationcontroller", r"NOTE .*ReplicationController/fixture/big x4 256\.00GiB"),
        ("unmodelled-flink", r"FAIL .*FlinkDeployment/fixture/flink: kind FlinkDeployment .*not modelled"),
        ("cnpg-no-limit", r"FAIL .*Cluster/fixture/pg: no memory limit"),
        ("cnpg-shared-buffers", r"FAIL .*shared_buffers 16GB does not fit its memory limit 512Mi"),
        ("hpa-max", r"NOTE .*HorizontalPodAutoscaler maxReplicas 10 counted"),
        ("empty", r"FAIL .*no workload found in the manifests"),
      ]],
    # profile schema
    *[("profile_schema", f"profile/{c}", f"profile/{c}", ["--root", "{root}"], e, rx, []) for c, e, rx in [
        ("good", 0, r"PASS profiles/fx-good/site\.yaml: valid"),
        ("secret-value", 1, r"FAIL .*oidc\.credential: must match exactly one of oneOf"),
        ("fact-typo", 1, r"FAIL .*unknown property 'catalog\.authn'"),
        ("cross-field", 1, r"FAIL .*connect\.sales\.api 'strimzi\.io/v1beta2' != facts\.streaming\.connector_api"),
        ("plain-http", 1, r"FAIL .*icebergRest\.uri: 'http://"),
        ("passed-before-any-run", 1, r"FAIL .*status: must be 'pending', got 'passed'"),
        ("wrong-reason-before-any-run", 1, r"FAIL .*reasonCode: must be 'not_run'"),
        ("stages-run", 0, r"PASS profiles/fx-run/site\.yaml: valid"),
        ("passed-no-evidence", 1, r"FAIL .*missing required property 'evidence'"),
        ("missing-ledger", 1, r"FAIL .*expectations expected/core\.yaml is missing"),
    ]],
    # secrets
    ("secret_scan", "secrets/planted-canary", "secrets/planted-canary", ["--root", "{root}"], 1,
     r"FAIL working tree: fluid-lab-canary at runner\.env:4", []),
    ("secret_scan", "secrets/planted-generic", "secrets/planted-generic", ["--root", "{root}"], 1,
     r"FAIL working tree: generic-api-key at values\.yaml:6", []),
    ("secret_scan", "secrets/clean", "secrets/clean", ["--root", "{root}"], 0, r"PASS gitleaks working tree", []),
    ("secret_scan", "secrets/empty-tree", "secrets/empty-tree", ["--root", "{root}"], 1,
     r"FAIL .*no files to scan \(an empty tree proves nothing\)", ["empty"]),
    # exposure (manifest fixtures are scanned in place; the kind config and the launcher only when given)
    ("exposure", "exposure/clean", None, ["--manifests", "{fx}/exposure/clean"], 0,
     r"PASS manifests: no NodePort, LoadBalancer or host port; SeaweedFS ClusterIP only, no admin server, "
     r"routes to seaweedfs-s3:8333 only", []),
    ("exposure", "exposure/service-nodeport", None, ["--manifests", "{fx}/exposure/service-nodeport"], 1,
     r"FAIL Service/fixture/exposed .*Service type NodePort", []),
    ("exposure", "exposure/hostport", None, ["--manifests", "{fx}/exposure/hostport"], 1,
     r"FAIL DaemonSet/fixture/listener .*hostPort 8080", []),
    ("exposure", "exposure/seaweedfs-admin", None, ["--manifests", "{fx}/exposure/seaweedfs-admin"], 1,
     r"FAIL Deployment/seaweedfs/seaweedfs-admin .*component 'admin' is rendered", []),
    ("exposure", "exposure/seaweedfs-admin (weed admin command)", None,
     ["--manifests", "{fx}/exposure/seaweedfs-admin"], 1, r"FAIL .*container admin runs `weed admin`", []),
    ("exposure", "exposure/seaweedfs-route", None, ["--manifests", "{fx}/exposure/seaweedfs-route"], 1,
     r"FAIL HTTPRoute/seaweedfs/master .*targets seaweedfs-master:9333", []),
    ("exposure", "exposure/envoy-loadbalancer", None, ["--manifests", "{fx}/exposure/envoy-loadbalancer"], 1,
     r"FAIL EnvoyProxy/envoy-gateway-system/lab .*envoyService type is LoadBalancer", []),
    ("exposure", "exposure/kind-wildcard", None,
     ["--manifests", "{fx}/exposure/clean", "--kind-config", "{fx}/exposure/kind-wildcard/kind.yaml"], 1,
     r"FAIL .*extraPortMappings\[0\]: listenAddress 0\.0\.0\.0", []),
    ("exposure", "exposure/launcher-wildcard", None,
     ["--manifests", "{fx}/exposure/clean", "--launcher", "{fx}/exposure/launcher-wildcard"], 1,
     r"FAIL .*launch\.py:\d+: `-p` publishes a port that does not start with 127\.0\.0\.1:", []),
    *[("exposure", f"exposure/launcher-host-shapes ({what})", None,
       ["--manifests", "{fx}/exposure/clean", "--launcher", "{fx}/exposure/launcher-host-shapes"], 1, rx, [])
      for what, rx in [
        ("host network", r"FAIL .*launch\.py:\d+: `--network host`: the container would listen on the host's"),
        ("attached -p", r"FAIL .*launch\.py:11: `-p` publishes a port that does not start with 127\.0\.0\.1:"),
        ("f-string --publish=", r"FAIL .*launch\.py:15: `--publish=` publishes a port that does not start with 127"),
        ("port-forward --address", r"FAIL .*launch\.py:\d+: `--address 0\.0\.0\.0`: a forwarded port must"),
    ]],
    ("exposure", "exposure/kind-api-wildcard", None,
     ["--manifests", "{fx}/exposure/clean", "--kind-config", "{fx}/exposure/kind-api-wildcard/kind.yaml"], 1,
     r"FAIL .*networking\.apiServerAddress 0\.0\.0\.0: the API server must listen on 127\.0\.0\.1", []),
    *[("exposure", f"exposure/incluster-shapes ({what})", None, ["--manifests", "{fx}/exposure/incluster-shapes"],
       1, rx, []) for what, rx in [
        ("Strimzi listener", r"FAIL Kafka/kafka/lab-kafka .*listener external has type nodeport"),
        ("CNPG service template", r"FAIL Cluster/lab-db/lab-pg .*serviceTemplate\.spec\.type is LoadBalancer"),
        ("Service in a List", r"FAIL List/bundle .*\.items\[0\]\.spec\.type is NodePort"),
        ("Ingress", r"FAIL Ingress/fixture/web .*an Ingress listens beyond the cluster"),
    ]],
    ("exposure", "exposure/gateway-no-envoyproxy", None, ["--manifests", "{fx}/exposure/gateway-no-envoyproxy"], 1,
     r"FAIL manifests: Gateway/envoy-gateway-system/lab .*rendered with no EnvoyProxy", []),
    # third-party notices
    *[("third_party", f"third-party/{c}", f"third_party/{c}", ["--root", "{root}"], e, rx, [])
      for c, e, rx in [
        ("clean", 0, r"PASS NOTICE reproduces the 1 NOTICE file"),
        ("stale-table", 1, r"FAIL THIRD-PARTY\.md table 3 .*missing: \| quay\.io/example/other:2\.0\.0"),
        ("extra-row", 1, r"FAIL THIRD-PARTY\.md table 3 .*row has no lock entry: \| quay\.io/example/other"),
        ("missing-notice", 1, r"FAIL platform/charts/demo/NOTICE: 3 line\(s\) not reproduced in NOTICE"),
      ]],
]
REPO_EXPECT = {  # what a real-repository run must print besides exit 0
    "render": r"PASS tier full:", "schema": r"PASS full:", "licence": r"PASS .*vendored chart",
    "denylist": r"PASS versions\.lock\.yaml:", "brand_fence": r"PASS \d+ text file", "reuse_lint": r"PASS reuse lint",
    "chart_app": r"PASS trino: a rendered container runs 483", "footprint": r"PASS full:",
    "profile_schema": r"PASS profiles/lab-kind/site\.yaml", "exposure": r"PASS full: no NodePort, LoadBalancer or host port",
    "third_party": r"PASS NOTICE reproduces", "secret_scan": r"PASS gitleaks working tree: no findings \(\d+ file",
}


def offline_env() -> dict:
    env = dict(os.environ)
    for v in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env[v] = "http://127.0.0.1:9"
    env["NO_PROXY"] = env["no_proxy"] = ""
    env["LAB_OFFLINE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_gate(gate: str, args: list) -> tuple:
    started = time.monotonic()
    res = subprocess.run([sys.executable, str(L0 / f"{gate}.py"), *args], cwd=str(REPO), env=offline_env(),
                         capture_output=True, text=True, errors="replace", timeout=1800, check=False)
    return res.returncode, res.stdout + res.stderr, time.monotonic() - started


def run_case(case: tuple, verbose: bool) -> bool:
    gate, cid, fixture, args, want_exit, want_rx, prep = case
    tmp = Path(tempfile.mkdtemp(prefix="l0-selftest-"))
    root = tmp / "root"
    try:
        if fixture:
            src = FX / fixture
            if not src.is_dir():
                print(f"  FAIL  {gate:<14} {cid}: fixture {src} missing")
                return False
            shutil.copytree(src, root, symlinks=True)
        else:
            root.mkdir()
        for step in prep:
            {"pack": pack_charts, "licenses": add_licenses, "tamper": tamper_chart, "pycache": pycache,
             "packnested": pack_nested, "goodchart": good_chart, "minilock": mini_lock,
             "repo-overlay": repo_overlay, "uncached": uncached_chart, "inject": inject_helmfile,
             "droptrino": drop_trino, "empty": empty_root}[step](root)
        argv = [a.replace("{root}", str(root)).replace("{fx}", str(FX)) for a in args]
        code, out, secs = run_gate(gate, argv)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    ok_exit = code == want_exit
    # Gate lines are "  PASS  msg"; spaces are collapsed so patterns use single spaces.
    ok_rx = re.search(want_rx, re.sub(r"[ \t]+", " ", out), re.M) is not None
    ok = ok_exit and ok_rx
    status = "PASS" if ok else "FAIL"
    detail = f"exit {code} (want {want_exit})" + ("" if ok_rx else f"; missing output /{want_rx}/")
    print(f"  {status}  {gate:<14} {cid:<52} {detail} [{secs:.1f}s]")
    if verbose or not ok:
        for line in out.strip().splitlines()[-25:]:
            print(f"          | {line}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="Run every L0 gate against its planted fixtures and the real repo.")
    ap.add_argument("--only", help="comma-separated gate names")
    ap.add_argument("--fixtures-only", action="store_true")
    ap.add_argument("--repo-only", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    all_gates, inventory_problems = gates()
    only = set(args.only.split(",")) if args.only else None
    unknown = (only or set()) - set(all_gates)
    if unknown:
        print(f"unknown gate(s): {', '.join(sorted(unknown))}; gates: {', '.join(all_gates)}")
        return 2
    results = []
    # Coverage: every gate needs a planted failing case and a real-repo run.
    print(f"L0 selftest: {len(all_gates)} gate(s): {', '.join(all_gates)}")
    for prob in inventory_problems:
        print(f"  FAIL  inventory      {prob}")
        results.append(False)
    for g in all_gates:
        if not any(cs[0] == g and cs[4] != 0 for cs in CASES):
            print(f"  FAIL  {g:<14} has no planted (failing) fixture case")
            results.append(False)
        if g not in REPO_EXPECT:
            print(f"  FAIL  {g:<14} has no real-repository expectation")
            results.append(False)
    # Every fixture case directory must be used by a case: an orphan fixture
    # is unreviewed content (and the brand fence allowlists fixture paths).
    used = set()
    for cs in CASES:
        if cs[2]:
            used.add("/".join(cs[2].split("/")[:2]))
        for a in cs[3]:
            if "{fx}/" in a:
                used.add("/".join(a.split("{fx}/", 1)[1].split("=")[0].split("/")[:2]))
    for area in sorted(p for p in FX.iterdir() if p.is_dir()):
        for case_dir in sorted(p for p in area.iterdir() if p.is_dir()):
            key = f"{area.name}/{case_dir.name}"
            if key not in used:
                print(f"  FAIL  fixtures       tests/fixtures/{key} is used by no selftest case (orphan)")
                results.append(False)
    if not args.repo_only:
        print("Fixtures (planted cases must fail, good cases must pass):")
        for case in CASES:
            if only and case[0] not in only:
                continue
            results.append(run_case(case, args.verbose))
    if not args.fixtures_only:
        print("Real repository (every gate must pass):")
        for g in all_gates:
            if only and g not in only:
                continue
            results.append(run_case((g, "real repository", None, [], 0, REPO_EXPECT.get(g, r"gate"), []),
                                    args.verbose))
    passed = sum(1 for r in results if r)
    print(f"L0 selftest: {'PASS' if all(results) and results else 'FAIL'} ({passed}/{len(results)} case(s) as expected)")
    return 0 if results and all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
