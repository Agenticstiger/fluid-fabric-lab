<!--
SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
SPDX-License-Identifier: Apache-2.0
-->

# L0 gates

Offline checks that run on every pull request. Each gate is one standard-library Python script (3.10+). Exit 0 means
pass, 1 means the check found a problem, and 2 means the check could not run
(for example, a tool is missing). A gate never exits 0 on a skip, and a gate
that checked nothing fails.

```sh
./lab l0                       # every gate, through the launcher (offline env)
./lab l0 footprint licence     # some gates
python3 tools/l0/<gate>.py -h  # one gate directly
python3 tools/l0/selftest      # planted fixtures + the real repository
```

## Registry and summaries

Every gate is listed in `gates.conf`. `lab l0` and the selftest fail when a
gate in this directory is unregistered, a registered gate is missing, a `.py`
has a non-standard `__main__` guard (it would never run), a `.sh` is not
executable, or any other unexpected file sits here. A gate that exits 0
passes only if it printed `gate <name>: PASS (N check(s))` with N >= 1, so a
bare `sys.exit(0)` stub fails. The selftest also fails on a fixture directory
that no case uses.

## No network

Every subprocess gets every proxy variable pointed at a closed loopback port
(`127.0.0.1:9`). Any tool that honours proxies and tries the network fails
instead of silently succeeding. The selftest proves this with an OCI chart pull
that stops with `proxyconnect ... 127.0.0.1:9`. Proxy poisoning does not stop
a process that opens a socket itself, so the render also refuses, before
running anything, a Helmfile with a network-capable template function
(`exec`, secret fetchers), remote composition or a hook other than the cache
check. CI runs every gate in a network namespace with only loopback; a local
run has no such namespace ([docs/proven.md](../../docs/proven.md#offline-checks-l0), known limits).

## Tools

The gates use pinned CLI tools (see `versions.lock.yaml` `tools`) instead of
new code: helm and helmfile through `labctl/render.py`, which the gates reuse;
kubeconform; reuse; gitleaks; trivy, for its licence classifier; and yq, for
YAML.

## Gates

| Gate | Checks | Planted cases (tests/fixtures/) |
|---|---|---|
| `render` | `helmfile template` renders every tier, plus the kafbat-ui add-on, from the committed chart cache: the cache (dotfiles included) is checked against SHA256SUMS and the lock; the Helmfile is refused statically if it can reach the network; every release's chart must be one the lock pins in the cache (or the glue chart); and each tier must enable every release whose chart the lock puts in that tier | remote repo chart, remote OCI chart, tampered archive, dotfile in a cached chart, chart outside the cache, `exec` in the Helmfile, a tier that drops a release |
| `schema` | `regenerate.sh verify`, then kubeconform `-strict` against the vendored schemas only. There is no remote location and a missing schema is an error. `MISSING.txt` kinds are a named, reviewed `-skip` | unknown field, kind with no schema |
| `licence` | Reads licence text in every chart (nested subcharts and subchart archives too), in `platform/` and in `LICENSES/`, and sweeps the full text of every file in the cache and in the tree (but `.git/`, `tests/` and the gate itself) for denied licence titles, whatever its name or SPDX header; licence-named files are also read for short ids (an AGPL rider). Fails a tree with no chart cache or lock, and any chart or archive outside the cache. Checks `reuse spdx` per-file licences and lock licence records. OSL-3.0 is denied when vendored and allowed only as a runtime reference with a real digest and real `obligations` | BUSL, SSPL, AGPL, unlicensed, EULA, vendored OSL (text and header), runtime OSL with and without obligations, placeholder OSL record, UBI EULA, Apache, renamed licence file, lying header, nested subchart archive, AGPL rider, chart outside the cache, no cache |
| `denylist` | Denied images in every string of the rendered manifests (registry-agnostic, base64 decoded, denied org names on their own), in the lock, and in Helmfile/values/scripts text. Flags `:latest`, untagged and digest-less images in containers of every workload kind and in image fields of custom resources | each denied image family, mirrored lakekeeper-plus, ghcr Lakekeeper postgresql, Percona MongoDB, split org/name, base64 Secret, silo-arm64, `:latest`/untagged in CRs and a ReplicationController, `:latest`, untagged, tag-only, env-var image, lock `use: used`, chart-repo URL |
| `brand_fence` | Third-party names in text and paths, outside `brand-fence.conf` paths and outside `brand-fence: allow` lines or blocks. Substring match with separators allowed, NFKC + invisible characters dropped + lookalikes folded, base64 decoded, UTF-16 and NUL-containing text read, `__pycache__` walked | planted text, planted path, unterminated block, dash/dot/underscore/zero-width/Cyrillic spellings, joined forms, base64, UTF-16, NUL byte, file hidden in `__pycache__`, allowlisted lines |
| `reuse_lint` | `reuse lint`, plus `profiles/<overlay>/` values-only rules and `reuse lint-file` | copied CRD, kustomize patch, no header, REUSE.toml-only, missing info |
| `chart_app` | Chart.yaml version and appVersion against the lock, `runs` against appVersion (allowlist with reason), subcharts recorded (directories and archives), `helm.sh/chart` label, rendered images against the lock repo, tag and digest. Flags `default-unused` images that are run | appVersion drift, unallowed runs, tag drift, digest drift, default-unused rendered, unrecorded subchart archive |
| `footprint` | Per tier: every container needs a memory limit; max(limit, heap + direct + overhead) with heap flags read from args, env, envFrom, key refs and mounted ConfigMaps/Secrets; replicas or HPA maxReplicas; every built-in workload kind; operator CRs modelled, any other kind fails; an empty render fails; plus estimates and `outside_cluster_gib`, against `memory_target_gib` (`-v` itemises) | Trino with 2 workers and 8G heaps, the same heap hidden in additionalJVMConfig with limits removed, no limit, heap via configMapKeyRef / envFrom, ReplicationController, FlinkDeployment, CNPG with no limit / oversized shared_buffers, HPA, empty render, unbounded JVM, unmodelled CR |
| `profile_schema` | `profiles/**/site.yaml` and `expected/*.yaml` against `profiles/schema/*.json`, plus the schema's `x-crossFieldRules`. A rule the gate does not implement fails | credential value, fact typo, cross-field mismatch, plain http, `passed` while `stagesRun` is false, `passed` without evidence, missing ledger |
| `exposure` | Nothing the lab runs listens beyond this machine. On every rendered tier: no NodePort or LoadBalancer Service and no `externalIPs`; the Envoy Service (`EnvoyProxy`) is NodePort 30443 only; no `hostNetwork`, `hostPID`, `hostIPC` or `hostPort` in any object; SeaweedFS Services are ClusterIP, only the master, volume, filer and s3 workloads run (no `weed admin`, no worker) and every Gateway route into `seaweedfs` targets `seaweedfs-s3:8333` (the S3 API, where STS also lives). No other object asks for an exposed Service: no `type: NodePort` or `LoadBalancer` anywhere (operator custom resources, `List` items), no Strimzi listener other than `internal` or `cluster-ip`, no `Ingress`, and no Gateway without an `EnvoyProxy`. Also: every kind `extraPortMappings` entry listens on 127.0.0.1 and maps containerPort 30443, and the API server address, when set, is 127.0.0.1; in `labctl/*.py` every Docker port publication (separate, `=`, attached or f-string form) binds 127.0.0.1, no container joins the host network, and every `--address` is 127.0.0.1 | NodePort Service, hostPort, SeaweedFS admin server, route to the SeaweedFS master, Envoy LoadBalancer, Gateway with no EnvoyProxy, Strimzi `nodeport` listener, CloudNativePG LoadBalancer template, NodePort Service inside a `List`, Ingress, kind `0.0.0.0` mapping, kind API server on `0.0.0.0`, launcher `-p` without 127.0.0.1 (separate, attached and f-string), `--network host`, port-forward `--address 0.0.0.0` |
| `third_party` | `THIRD-PARTY.md` and `NOTICE` cover every vendored chart and image. Each `<!-- yq: ... -->` table is compared row by row with the output of that expression on `versions.lock.yaml`; the chart cache (`SHA256SUMS`) and the lock list the same charts; every NOTICE file inside a vendored chart is reproduced in `NOTICE` | lock image with no row, row with no lock entry, chart NOTICE missing from the root NOTICE |
| `secret_scan` | gitleaks (`.gitleaks.toml`: default rules plus the lab canary) on the working tree, and on the history once commits exist; an empty tree fails | canary, generic secret, empty tree |

The launcher's own bypasses (a stub gate, an unregistered gate, a
non-standard guard, a non-executable `.sh`) are pinned by
`tests/launcher/test_l0.py`, which CI runs on Python 3.10 and 3.14.

The schema checks are split in two:
`schema` covers Kubernetes manifests and `profile_schema` covers site profiles.

## Choices worth knowing

- **JSON Schema.** No JSON Schema CLI is in the toolchain, so
  `_jsonschema.py` implements the keyword subset that the profile schemas
  declare they use. It refuses any other keyword, so a schema that outgrows
  the subset fails loudly.
- **Licence text.** trivy's classifier (Google licenseclassifier) recognises
  full canonical texts. A stdlib table of title phrases catches denied
  families, EULAs and short or modified texts that the classifier does not
  match. If either one denies a text, the gate denies it. A text that
  neither one recognises fails.
- **Footprint estimates.** Some workloads that tiers.yaml counts are not
  rendered: the kind node at 1.0 GiB and the runner Job at
  0.5 GiB. Their estimates are constants in `footprint.py`, marked as
  estimates.
- **Fixture isolation.** The selftest copies each fixture to a temporary
  directory, so the repository's `tests/fixtures/` allowlists (in REUSE.toml,
  `.gitleaks.toml` and `brand-fence.conf`) do not hide the planted problems.
