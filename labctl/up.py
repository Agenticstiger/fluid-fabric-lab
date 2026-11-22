# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""`lab up`: bring a tier up on kind, from the local mirror only.

Steps, in order; the first failure stops the run, names the step and says
what to do. Every step is idempotent, so re-running `lab up` resumes.

  preflight   runtime reachable, the tier fits the VM (doctor's checks),
              required tools present, built images the tier needs exist
  mirror      the registry runs and holds every image of the tier by digest
  pki         the lab CA (host, state directory) and OpenBao's certificate
  cluster     the lab's own IPv4-only Docker network, then kind on it from
              the mirror's node image; containerd pinned to the mirror with
              no upstream fallback; CoreDNS rewrites
  openbao     sibling container on that network: init (once), unseal,
              configure; the lab's credentials are generated once and kept there
  secrets     namespaces (Pod Security labels) and every Secret the charts
              read, from OpenBao, on kubectl's stdin
  helmfile    `helmfile sync` of the tier (charts from the committed cache); the
              orchestration tier's Airflow is its own last wave, after the
              airflow-fluid image check and the S3 prefixes it reads (labctl.airflow)
  bootstrap   the warehouse bucket; Lakekeeper bootstrap and the `sales-wh`
              warehouse with STS vending
  measure     memory of the node and the sibling containers (docker stats)

Nothing is ever skipped: a step that cannot run is a failure.
"""

from __future__ import annotations

import json
import os
import platform as _platform
import re
import shutil
import time
from pathlib import Path
from typing import Callable, Optional

from labctl import (airflow, bootstrap, cluster, config, containers, credentials, doctor, down, http, kube,
                    mirror, openbao, pki, proc, s3, spark, state)
from labctl.output import FAIL, PASS, WARN, check

TOOLS = ("docker", "kind", "kubectl", "helm", "helmfile", "crane", "openssl", "yq")
# Tiers `lab up` can bring up today: core, the spark add-on (its smoke run is
# `lab check --tier spark`), the orchestration add-on
# (Airflow; needs the airflow-fluid image from `lab build`) and the streaming add-on
# (`lab check --tier streaming`). The streaming tier brings up Kafka only:
# Strimzi generates its users' passwords, and KafkaConnect waits for the
# connect-iceberg image, which the lab does not build.
SUPPORTED_TIERS = ("core", "spark", "streaming", "orchestration", "e2e")
HELMFILE_TIMEOUT_S = 2400
CREDENTIALS_PATH = "lab/credentials"   # kv/data/lab/credentials in OpenBao
WEBHOOK_ATTEMPTS = 3
WEBHOOK_RETRY_S = 20
_WEBHOOK_WARMUP = re.compile(r"failed calling webhook .*(connection refused|no endpoints available)")


def is_webhook_warmup(output: str) -> bool:
    """True when a Helm failure is a webhook that is not answering yet."""
    return bool(_WEBHOOK_WARMUP.search(output or ""))


class StepError(Exception):
    def __init__(self, message: str, advice: str = ""):
        super().__init__(message)
        self.advice = advice


class Up:
    def __init__(self, root: Path, tier: str, *, name: str = state.DEFAULT_NAME, https_port: int = 443,
                 mirror_port: int = mirror.DEFAULT_PORT, runner: proc.Runner = proc.run,
                 env: Optional[dict] = None, log: Callable[[str], None] = lambda _m: None):
        self.root = root
        self.tier_name = tier
        self.https_port = https_port
        self.mirror_port = mirror_port
        self.runner = runner
        self.env = dict(os.environ if env is None else env)
        self.log = log
        self.lab = state.lab_for(root, name, self.env)
        self.kube = kube.Kube(self.lab, runner)
        self.checks: list = []
        self.lock: dict = {}
        self.tiers_doc: dict = {}
        self.tier: Optional[config.Tier] = None
        self.entries: list = []
        self.creds: dict = {}
        self.bao: Optional[openbao.Bao] = None
        self.measure: dict = {}

    # ------------------------------------------------------------------ driver

    def run(self) -> dict:
        # The cluster step creates the lab's Docker network, which the OpenBao
        # container joins, so it runs first.
        steps = [("preflight", self.preflight), ("mirror", self.mirror), ("pki", self.pki),
                 ("cluster", self.cluster), ("openbao", self.openbao), ("secrets", self.secrets),
                 ("helmfile", self.helmfile), ("bootstrap", self.bootstrap), ("measure", self.measure_step)]
        ok = True
        for name, fn in steps:
            self.log(f"lab up: {name} ...")
            started = time.monotonic()
            try:
                message = fn()
                self.checks.append(check(f"up:{name}", PASS, message,
                                         detail={"seconds": round(time.monotonic() - started, 1)}))
            except StepError as exc:
                self.checks.append(check(f"up:{name}", FAIL, str(exc), exc.advice))
                ok = False
                break
            except (bootstrap.BootstrapError, airflow.AirflowError) as exc:
                self.checks.append(check(f"up:{name}", FAIL, str(exc), exc.advice or
                                         "Fix the cause above and re-run `lab up`; finished steps are reused."))
                ok = False
                break
            except (config.ConfigError, mirror.MirrorError, pki.PKIError, openbao.BaoError,
                    cluster.ClusterError, kube.KubeError, http.HTTPError, s3.S3Error, state.StateError,
                    down.NotALab, containers.DockerError) as exc:
                self.checks.append(check(f"up:{name}", FAIL, str(exc),
                                         "Fix the cause above and re-run `lab up`; finished steps are reused."))
                ok = False
                break
            except (OSError, ValueError, KeyError) as exc:
                # Not an expected failure: a damaged state file, an answer that
                # is not JSON, a missing key. Still one record, never a traceback.
                self.checks.append(check(f"up:{name}", FAIL, f"unexpected {type(exc).__name__}: {exc}",
                                         f"Check the state directory {self.lab.state}; if this repeats, it is "
                                         "a launcher defect worth reporting with this output."))
                ok = False
                break
        if ok:
            self.checks.append(self._host_port_check())
        return {"command": "up", "tier": self.tier_name, "name": self.lab.name,
                "state_dir": str(self.lab.state), "kubeconfig": str(self.lab.kubeconfig),
                "https_port": self.https_port, "measured": self.measure, "ok": ok, "checks": self.checks}

    # ------------------------------------------------------------------ steps

    def preflight(self) -> str:
        self.lock = config.load_yaml(self.root / config.VERSIONS_LOCK, self.runner)
        self.tiers_doc = config.load_yaml(self.root / config.TIERS, self.runner)
        self.tier = config.resolve_tier(self.tiers_doc, self.tier_name)
        if self.tier_name not in SUPPORTED_TIERS:
            raise StepError(f"tier {self.tier_name!r} is not supported by `lab up` (supported: "
                            f"{', '.join(SUPPORTED_TIERS)})",
                            "Its charts need Secrets or images the lab does not provide "
                            "(docs/proven.md, Not proven yet).")
        missing = [t for t in TOOLS if not shutil.which(t)]
        if missing:
            raise StepError(f"missing tools: {', '.join(missing)}", "Install them (versions in versions.lock.yaml) "
                            "and run `lab doctor`.")
        rt = doctor.select_runtime(None, self.env, proc.which)
        if rt != "docker":
            # The lab network, OpenBao, `docker exec` into the node and the
            # measurements all use the docker CLI; podman is not supported.
            raise StepError(f"the lab runs with the docker runtime only (selected: {rt})",
                            "Unset KIND_EXPERIMENTAL_PROVIDER and use Docker Desktop, Colima, Rancher "
                            "Desktop (moby) or Docker Engine.")
        rt_check, vm_bytes, _ = doctor.probe_runtime(rt, self.runner)
        if rt_check["status"] == FAIL:
            raise StepError(rt_check["message"], rt_check.get("advice", ""))
        all_tiers = {n: config.resolve_tier(self.tiers_doc, n) for n in config.tier_names(self.tiers_doc)}
        mem = doctor.memory_checks(self.tier, all_tiers, vm_bytes, None, rt, None)[0]
        if mem["status"] == FAIL:
            raise StepError(mem["message"], mem.get("advice", ""))
        disk = doctor.disk_check(self.root, doctor.DEFAULT_MIN_FREE_DISK_GIB)
        if disk["status"] == FAIL:
            raise StepError(disk["message"], disk.get("advice", ""))
        return f"{mem['message']}; {disk['message']}"

    def mirror(self) -> str:
        reg = mirror.ensure_registry(self.runner, self.lock, self.mirror_port, create=False)
        if reg["status"] == FAIL:
            raise StepError(reg["message"], reg.get("advice", ""))
        self.entries = mirror.tier_images(self.lock, self.tiers_doc, self.tier_name)
        absent = mirror.missing(self.runner, self.entries, self.mirror_port)
        if absent:
            raise StepError(f"the mirror lacks {len(absent)} pinned image(s): "
                            + ", ".join(e["ref"] for e in absent[:6]) + (" ..." if len(absent) > 6 else ""),
                            f"Run `./lab mirror --tier {self.tier_name}` (it needs the network once).")
        return f"{len(self.entries)} image(s) of tier {self.tier_name!r} in the mirror (digests verified)"

    def pki(self) -> str:
        down.mark_state_dir(self.lab)
        ca_crt, _ = pki.ensure_ca(self.runner, self.lab.state / "ca", self.lab.name)
        tls = self.lab.state / "openbao" / "tls"
        if not (tls / "tls.crt").is_file():
            pki.issue(self.runner, self.lab.state / "ca", tls, openbao.STABLE_HOST,
                      dns_names=[openbao.STABLE_HOST, self.lab.openbao, "localhost"], ips=["127.0.0.1"])
            shutil.copyfile(ca_crt, tls / "ca.crt")
        # The container's non-root user reads these through a bind mount, so
        # they are group/world-readable; the 0700 directories above them keep
        # every other host user out.
        os.chmod(tls, 0o755)
        for item in tls.iterdir():
            os.chmod(item, 0o644)
        return f"lab CA {ca_crt}"

    def openbao(self) -> str:
        tls = self.lab.state / "openbao" / "tls"
        started = openbao.run_container(self.runner, self.lab, self.lock, tls,
                                        self.root / "platform/keys/openbao/config.hcl", self.mirror_port)
        port = openbao.host_port(self.runner, self.lab.openbao)
        self.bao = openbao.Bao(port, str(self.lab.state / "ca" / "ca.crt"))
        steps = openbao.bring_up(self.bao, self.lab)
        existing = self.bao.kv_get(openbao.KV_MOUNT, CREDENTIALS_PATH) or {}
        self.creds, changed = credentials.generate(existing)
        if changed:
            self.bao.kv_put(openbao.KV_MOUNT, CREDENTIALS_PATH, self.creds)
        return f"{started}; {steps}; credentials {'generated' if changed else 'reused'} (kv/lab/credentials)"

    def cluster(self) -> str:
        cfg = cluster.runtime_config(self.root, self.lab, self.lock, self.https_port, self.mirror_port, self.runner)
        net = cluster.ensure_network(self.runner, self.lab)
        existed = cluster.exists(self.runner, self.lab)
        created = cluster.create(self.runner, self.lab, cfg, self.env)
        if not existed:
            # A new node holds none of the images `lab build` loaded into the old one.
            self.lab.images_record.unlink(missing_ok=True)
        mapped = cluster.gateway_host_port(self.runner, self.lab)
        if mapped and mapped != self.https_port:
            # Host ports are fixed when kind creates the node; report the real one.
            created += f" (its Gateway host port is {mapped}, fixed at creation; --https-port {self.https_port} ignored)"
            self.https_port = mapped
        hosts = cluster.pin_pulls_to_mirror(self.runner, self.lab, mirror.upstream_hosts(mirror.all_images(self.lock)))
        cm = self.kube.get("configmap", "coredns", "kube-system")
        if not cm:
            raise StepError("CoreDNS ConfigMap kube-system/coredns not found")
        new = cluster.corefile_with_rewrites(cm["data"]["Corefile"], self.lab)
        if new != cm["data"]["Corefile"]:
            cm["data"]["Corefile"] = new
            for key in ("resourceVersion", "uid", "creationTimestamp", "managedFields"):
                cm["metadata"].pop(key, None)
            self.kube.apply([cm])
            self.kube.run("-n", "kube-system", "rollout", "restart", "deployment/coredns")
            self.kube.run("-n", "kube-system", "rollout", "status", "deployment/coredns", "--timeout=180s",
                          timeout=210)
        return f"{net}; {created}; pulls pinned to the mirror for {', '.join(hosts)}; *.lab.test rewrites in CoreDNS"

    def secrets(self) -> str:
        nss = cluster.tier_namespaces(self.root, self.runner, self.tiers_doc, self.tier_name)
        self.kube.apply([cluster.namespace(n, s.get("pod_security", "restricted"), s.get("exemption", ""))
                         for n, s in nss.items()])
        ca_dir = self.lab.state / "ca"
        objs = credentials.secrets_for(self.creds, set(nss))
        objs.append(kube.secret("lab-ca", "cert-manager", {
            "tls.crt": (ca_dir / "ca.crt").read_text(), "tls.key": (ca_dir / "ca.key").read_text(),
            "ca.crt": (ca_dir / "ca.crt").read_text()}, kind="kubernetes.io/tls"))
        self.kube.apply(objs)
        return (f"{len(nss)} namespace(s), {len(objs) - 1} Secret(s) from OpenBao and the lab CA Secret "
                f"applied")

    def helmfile(self) -> str:
        env = dict(self.env)
        env["KUBECONFIG"] = str(self.lab.kubeconfig)
        # Helm keeps its caches inside the state directory, never in ~/.cache.
        for var, sub in (("HELM_CACHE_HOME", "helm/cache"), ("HELM_CONFIG_HOME", "helm/config"),
                         ("HELM_DATA_HOME", "helm/data")):
            env[var] = str(state.ensure_dir(self.lab.state / sub))
        if "lab-spark" in self.tier.releases:
            # fluid-host runs the image `lab build fluid-runner` recorded; no record, no fluid-host yet.
            env[spark.RUNNER_ENV] = spark.recorded_runner(self.lab.images_record)
            # The Spark pods run the spark-iceberg image `lab build` recorded.
            env[spark.IMAGE_ENV] = spark.recorded_image(self.lab.images_record, spark.APACHE_IMAGE_ID)
            if not env[spark.IMAGE_ENV]:
                # A first `lab up` cannot have the record: it is written when `lab build` loads the image into this
                # lab's node, so the order is up, build both images, up again. Until then fluid-host is not rendered.
                self.log(f"lab up: no spark-iceberg image is recorded for lab {self.lab.name} yet, so fluid-host is "
                         f"not rendered and lab-spark-submit has no image; run `./lab build fluid-runner --name "
                         f"{self.lab.name}` and `./lab build spark-iceberg --name {self.lab.name}`, then `./lab up "
                         f"--tier {self.tier_name} --name {self.lab.name}` again")
        log = self.lab.state / "helmfile-sync.log"
        log.write_text("", encoding="utf-8")
        # Two waves. Lakekeeper exits at start when Keycloak's OIDC discovery
        # does not answer yet, and Helm cannot wait for a custom resource such
        # as the Keycloak CR, so wave 1 installs everything else, the launcher
        # waits for Keycloak and the realm import, and wave 2 installs
        # Lakekeeper. Ordering by crash-loop backoff would be a race.
        orchestration = "airflow" in self.tier.releases
        lab_images: list = []
        if orchestration:
            # Fail before the long first wave, not after a helm timeout: the Airflow pods never pull their image.
            # The chart renders the pinned base image offline; the lab runs the one `lab build` loaded.
            record = airflow.loaded_image(self.runner, self.lab, self.lock)
            lab_images = ["--state-values-set", f"labImages.airflow={record['tag']}"]
        waves = [("component!=lakekeeper" + (",component!=airflow" if orchestration else ""), "platform")]
        if "lakekeeper" in self.tier.releases:
            waves.append(("component=lakekeeper", "catalog"))
        if orchestration:
            waves.append(("component=airflow", "orchestration"))
        for selector, what in waves:
            if what == "catalog":
                bootstrap.wait_keycloak(self.kube)
            if what == "orchestration":
                # The bucket, its prefixes and the identity Airflow reads them with must exist before its
                # dag-processor starts (an S3DagBundle refuses a prefix that is not there).
                with bootstrap.gateway(self.kube) as fwd:
                    self.log("lab up: " + airflow.prepare(
                        self.kube, bootstrap.LabClient(self.lab.state / "ca" / "ca.crt", fwd.port, self.creds),
                        self.creds, self.root))
            argv = ["helmfile", "-f", str(self.root / config.HELMFILE), "-e", self.tier_name, *lab_images,
                    "--kube-context", self.lab.context, "-l", selector, "sync", "--skip-deps"]
            for attempt in range(1, WEBHOOK_ATTEMPTS + 1):
                res = self.runner(argv, env=env, cwd=str(self.root), timeout=HELMFILE_TIMEOUT_S)
                with log.open("a", encoding="utf-8") as handle:
                    handle.write(f"===== wave {what} ({selector}), attempt {attempt}\n{res.stdout}{res.stderr}")
                # A webhook whose Deployment is Ready can still refuse
                # connections for a few seconds (trust-manager, cert-manager,
                # 2026-11-18). Only that exact error is retried; every other
                # failure stops the run at once.
                if res.ok or not is_webhook_warmup(res.output_tail(20000)) or attempt == WEBHOOK_ATTEMPTS:
                    break
                self.log(f"lab up: a webhook was not answering yet; retrying the {what} wave ({attempt})")
                time.sleep(WEBHOOK_RETRY_S)
            if not res.ok:
                raise StepError(f"helmfile sync ({what} wave) failed: {res.output_tail(1500).strip()}",
                                f"Full log: {log}. `kubectl --kubeconfig {self.lab.kubeconfig} get pods -A` "
                                "shows what is not ready; re-run `lab up` to resume.")
        return (f"the tier's {len(self.tier.releases)} chart release(s) and their lab-object releases synced "
                f"in {len(waves)} wave(s) (log: {log})")

    # ------------------------------------------------------------------ bootstrap

    def bootstrap(self) -> str:
        done = []
        if "seaweedfs" in self.tier.releases:
            done.append(bootstrap.warehouse_bucket(self.kube))
        if "lab-spark" in self.tier.releases:
            done.append(bootstrap.artifacts_bucket(self.kube))
        if "lakekeeper" in self.tier.releases:
            with bootstrap.gateway(self.kube) as fwd:
                client = bootstrap.LabClient(self.lab.state / "ca" / "ca.crt", fwd.port, self.creds)
                done.append(bootstrap.lakekeeper(client))
                if "trino" in self.tier.releases:
                    done.append(bootstrap.keycloak_reconcile(client, self.kube))
                    done.append(bootstrap.trino_grant(client))
                if "lab-spark" in self.tier.releases:
                    done.append(bootstrap.spark_grant(client))
        if "strimzi-kafka-operator" in self.tier.releases:
            # Helm cannot wait for the Kafka CR; Ready means the KRaft node serves and both listeners are up.
            try:
                self.kube.wait("condition=Ready", "kafka/lab-kafka", "kafka", timeout_s=900)
            except kube.KubeError as exc:
                raise StepError(f"Kafka lab-kafka did not become ready: {exc}",
                                "`kubectl -n kafka describe kafka lab-kafka` shows why; re-run `lab up` to resume.") from exc
            done.append("Kafka lab-kafka is Ready")
        return "; ".join(done) or "nothing to bootstrap in this tier"

    def _host_port_check(self) -> dict:
        """Does the host port kind maps reach the Gateway? Browsers need it; the launcher does not."""
        why = bootstrap.host_port_reaches_gateway(self.lab.state / "ca" / "ca.crt", self.https_port)
        if why is None:
            return check("up:host-port", PASS, f"https://*.lab.test answers on 127.0.0.1:{self.https_port}")
        return check("up:host-port", WARN, f"127.0.0.1:{self.https_port} does not reach the lab Gateway ({why[:160]})",
                     "The lab works without it, but a browser cannot reach *.lab.test. On Docker Desktop a "
                     "privileged-port forward can go stale after a cluster is recreated; restarting Docker "
                     "Desktop clears it, or use `lab up --https-port <high port>` on a new lab.")

    # ------------------------------------------------------------------ measure

    def measure_step(self) -> str:
        names = [self.lab.node, self.lab.openbao, mirror.REGISTRY]
        res = self.runner(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names], timeout=120)
        if not res.ok:
            raise StepError(f"docker stats failed: {res.output_tail(300).strip()}")
        usage = {}
        for line in res.stdout.splitlines():
            row = json.loads(line)
            usage[row.get("Name")] = row.get("MemUsage")
        self.measure = {"tier": self.tier_name, "taken": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "host": _platform.platform(), "docker_stats_mem_usage": usage}
        state.write_json_private(self.lab.state / f"measured-{self.tier_name}.json", self.measure)
        return "; ".join(f"{k} {v}" for k, v in usage.items())


def run_up(root: Path, tier: str, **kwargs) -> dict:
    return Up(root, tier, **kwargs).run()
