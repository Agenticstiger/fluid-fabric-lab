# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Command-line entry point for `./lab`.

Exit codes: 0 success; 1 a check failed or could not be evaluated; 2 usage
error, or a request the command does not offer.

Each command has one parser builder (`_add_<command>`) and one handler
(`_run_<command>`); `main` only dispatches.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable, Optional, Sequence

from labctl import EXIT_FAIL, EXIT_OK, EXIT_USAGE, __version__, config
from labctl.output import emit_human, emit_json

DEFAULT_NAME = "fluid-fabric-lab"
DEFAULT_REGISTRY_PORT = 5001  # labctl.mirror.DEFAULT_PORT, kept here so --help imports nothing heavy
BUILDABLE = ("fluid-runner", "airflow-fluid", "spark-iceberg")  # labctl.build.IMAGES, for the same reason
ACCEPT_PROFILE = "lab-kind"    # labctl.accept.DEFAULT_PROFILE, for the same reason


def _add_doctor(sub) -> None:
    d = sub.add_parser("doctor", help="check runtime, tool pins, memory for a tier, and disk")
    d.add_argument("--tier", default="core", help="tier from tiers.yaml (default: core)")
    d.add_argument("--cluster", choices=("kind", "existing"), default="kind")
    d.add_argument("--runtime", choices=("docker", "podman"),
                   help="container runtime (default: KIND_EXPERIMENTAL_PROVIDER, else docker, else podman)")
    d.add_argument("--strict", action="store_true", help="treat warnings (e.g. a tool version off its pin) as failures")
    d.add_argument("--min-free-disk-gib", type=float, default=None,
                   help="free-disk floor in GiB (default 15)")
    d.add_argument("--json", action="store_true", help="machine-readable output")


def _add_render(sub) -> None:
    r = sub.add_parser("render", help="helmfile template for a tier, offline, from the committed chart cache")
    r.add_argument("--tier", default="core", help="tier from tiers.yaml (default: core)")
    r.add_argument("--out", help="output directory (must be empty or new; default: a new temp directory)")
    r.add_argument("--helmfile", help=f"helmfile path (default: {config.HELMFILE})")
    r.add_argument("--environment", help="helmfile environment name (default: the tier; the tiers are the helmfile environments)")
    r.add_argument("--json", action="store_true", help="machine-readable output")


def _add_l0(sub) -> None:
    l0 = sub.add_parser("l0", help="run the L0 gates in tools/l0/ (no network)")
    l0.add_argument("gates", nargs="*", help="gate names (default: all discovered)")
    l0.add_argument("--list", action="store_true", help="list the gates that would run")
    l0.add_argument("--timeout", type=float, default=None, help="per-gate timeout in seconds (default 900)")
    l0.add_argument("--json", action="store_true", help="machine-readable output")


def _add_mirror(sub) -> None:
    m = sub.add_parser("mirror", help="copy every pinned image of a tier into the local registry, by digest")
    m.add_argument("--tier", default="core", help="tier from tiers.yaml (default: core)")
    m.add_argument("--with-optional", action="store_true", help="also copy the optional add-on images")
    m.add_argument("--image", action="append", default=[], metavar="ID",
                   help="also copy this versions.lock.yaml image id (repeatable)")
    m.add_argument("--port", type=int, default=DEFAULT_REGISTRY_PORT,
                   help="host loopback port of the registry (default 5001)")
    m.add_argument("--json", action="store_true", help="machine-readable output")


def _add_build(sub) -> None:
    b = sub.add_parser("build", help="build a lab-owned image on this machine, check its contents, load it into a lab")
    b.add_argument("image", choices=BUILDABLE, help="the image to build (platform/images/<image>)")
    _lab_args(b, "lab whose kind node gets the image")


def _add_check(sub) -> None:
    c = sub.add_parser("check", help="prove each criterion of the tier on real evidence (exit 1 until all pass)")
    c.add_argument("--tier", default="core",
                   help="tier from tiers.yaml (default: core; an add-on tier adds its own record, such as `spark`)")
    c.add_argument("--only", action="append", default=[], metavar="RECORD",
                   help="check only this record of the tier (repeatable), e.g. `--only e2e`; the exit code then "
                        "judges those records alone")
    _lab_args(c, "lab name")


def _add_e2e(sub) -> None:
    e = sub.add_parser("e2e", help="bring the e2e tier up and run its end-to-end product through forge "
                                   "(the entry point `make lab-e2e` calls)")
    e.add_argument("--https-port", type=int, default=443,
                   help="host loopback port of the lab Gateway when the cluster is created (default 443)")
    e.add_argument("--registry-port", type=int, default=DEFAULT_REGISTRY_PORT,
                   help=f"mirror registry port (default {DEFAULT_REGISTRY_PORT})")
    e.add_argument("--skip-mirror", action="store_true",
                   help="do not run `lab mirror --tier e2e` first (the mirror already holds the tier)")
    e.add_argument("--down", action="store_true",
                   help="delete the lab's cluster afterwards (`lab down`, keeping the mirror and the state)")
    _lab_args(e, "lab name")


def _add_accept(sub) -> None:
    a = sub.add_parser(
        "accept",
        help="run forge's 11 stages on each example product and compare every result with the expectations ledger",
        description="Run `fluid ship --stage N` for each stage of each example product in the lab's fluid-host pod "
                    "(always strict, no override), then compare every observed status with the profile's expectations "
                    "ledger in both directions: an unexpected `passed` fails the run like an unexpected `failed`. "
                    "Writes conformance/reports/<date>-<profile>-<tier>-<run-id>.json and .md. "
                    "Exit 0 only if every cell matches; 1 on a mismatch or a run that could not be observed; "
                    "2 for a request the command does not offer.")
    _lab_args(a, "lab whose fluid-host runs forge")
    a.add_argument("--profile", default=ACCEPT_PROFILE, help=f"profile under profiles/ (default: {ACCEPT_PROFILE})")
    a.add_argument("--tier", help="tier of the profile's ledger (default: tiers.default of its site.yaml)")
    a.add_argument("--product", action="append", default=[], metavar="ID",
                   help="run only this product of the ledger (repeatable; default: every product in the ledger)")
    a.add_argument("--runner", default="cli", metavar="RUNNER",
                   help="how the stages run: `cli` runs fluid directly in fluid-host (the only runner "
                        "available; `dag` needs the orchestration tier and is refused)")
    a.add_argument("--env", default="local", help="forge environment of the contracts (default: local)")
    a.add_argument("--keep", action="store_true",
                   help="leave /work/accept/<run-id> in the fluid-host pod afterwards (default: remove it)")
    a.add_argument("--update-ledger", action="store_true",
                   help="write each observed status (or forge reason) that differs into the ledger file (evidence on passed "
                        "cells), keeping its comments and order, and print the diff; refused when the run had any "
                        "error; committing it with a message that names the capability change is still yours to do")


def _lab_args(parser, what: str) -> None:
    """--name and --json, which every command that acts on one lab takes."""
    parser.add_argument("--name", default=DEFAULT_NAME, type=_lab_name, help=f"{what} (default: {DEFAULT_NAME})")
    parser.add_argument("--json", action="store_true", help="machine-readable output")


def _lab_name(value: str) -> str:
    """argparse type for --name: a bad name is a usage error (exit 2), not a traceback."""
    from labctl import state

    try:
        return state.check_name(value)
    except state.StateError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_up(sub) -> None:
    u = sub.add_parser("up", help="create (or resume) a kind cluster and install a tier from the mirror")
    u.add_argument("--tier", default="core",
                   help="tier from tiers.yaml (default: core; `lab up` runs core and its add-on "
                        "tiers)")
    _lab_args(u, "lab name: prefixes the cluster, containers, volumes and state")
    u.add_argument("--cluster", choices=("kind",), default="kind",
                   help="kind only (an existing cluster is not supported)")
    u.add_argument("--https-port", type=int, default=443,
                   help="host loopback port of the lab Gateway (default 443; *.lab.test URLs assume 443)")
    u.add_argument("--registry-port", type=int, default=DEFAULT_REGISTRY_PORT,
                   help="mirror registry port (default 5001)")


def _add_down(sub) -> None:
    dn = sub.add_parser("down", help="delete one lab's cluster and sibling containers (never anything else)")
    _lab_args(dn, "lab name")
    dn.add_argument("--purge", action="store_true",
                    help="also delete the OpenBao volume and the state directory (CA, unseal shares): irreversible")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lab",
        description="fluid-fabric-lab launcher. Exit codes: 0 ok, 1 a check failed, 2 usage error.",
    )
    p.add_argument("--version", action="version", version=f"lab {__version__}")
    p.add_argument("--root", help="repository root (default: the directory containing ./lab)")
    sub = p.add_subparsers(dest="command", metavar="COMMAND")
    for add in (_add_doctor, _add_render, _add_l0, _add_mirror, _add_build, _add_up, _add_down, _add_check,
                _add_e2e, _add_accept):
        add(sub)
    return p


def _finish(doc: dict, as_json: bool, title: str) -> int:
    if as_json:
        emit_json(doc)
    else:
        emit_human(title, doc["checks"], doc["ok"])
        if doc.get("output_dir"):
            print(f"Manifests: {doc['output_dir']}")
    return EXIT_OK if doc["ok"] else EXIT_FAIL


# --------------------------------------------------------------------------- handlers


def _run_doctor(root, args) -> int:
    if args.cluster == "existing":
        sys.stderr.write(
            "lab doctor --cluster existing is not supported "
            "(needs node allocatable memory from the cluster). Use --cluster kind.\n"
        )
        return EXIT_USAGE
    from labctl import doctor

    kwargs = {"runtime": args.runtime, "strict": args.strict}
    if args.min_free_disk_gib is not None:
        kwargs["min_free_disk_gib"] = args.min_free_disk_gib
    doc = doctor.run_doctor(root, args.tier, **kwargs)
    return _finish(doc, args.json, f"lab doctor: tier {args.tier}, cluster kind, runtime {doc['runtime']}")


def _run_render(root, args) -> int:
    from labctl import render

    doc = render.run_render(root, args.tier, out=args.out, helmfile=args.helmfile, environment=args.environment)
    return _finish(doc, args.json, f"lab render: tier {args.tier} (offline, chart cache only)")


def _run_l0(root, args) -> int:
    from labctl import l0

    kwargs = {"list_only": args.list}
    if args.timeout is not None:
        kwargs["timeout"] = args.timeout
    doc = l0.run_l0(root, args.gates, **kwargs)
    return _finish(doc, args.json, f"lab l0: {config.L0_DIR}/ (offline)")


def _run_mirror(root, args) -> int:
    from labctl import mirror

    doc = mirror.run_mirror(root, args.tier, port=args.port, with_optional=args.with_optional,
                            extra_ids=args.image)
    return _finish(doc, args.json, f"lab mirror: tier {args.tier} -> 127.0.0.1:{args.port}")


def _progress(args) -> Callable[[str], None]:
    """Step lines on stderr for a person; nothing when the output is JSON."""
    return (lambda _m: None) if args.json else (lambda m: print(m, file=sys.stderr, flush=True))


def _state_guard(command: str, call: Callable[[], dict]) -> dict:
    """Run a command that opens a lab's state directory; a bad one is one FAIL record."""
    from labctl import state
    from labctl.output import FAIL, check

    try:
        return call()
    except state.StateError as exc:
        return {"command": command, "ok": False, "checks": [check(
            "state", FAIL, str(exc), "Point LAB_STATE_DIR at a new or empty directory outside the repository.")]}


def _run_build(root, args) -> int:
    from labctl import build

    doc = _state_guard("build", lambda: build.run_build(root, args.image, name=args.name, log=_progress(args)))
    return _finish(doc, args.json, f"lab build: {args.image}, lab {args.name}")


def _run_up(root, args) -> int:
    from labctl import up

    doc = _state_guard("up", lambda: up.run_up(root, args.tier, name=args.name, https_port=args.https_port,
                                              mirror_port=args.registry_port, log=_progress(args)))
    rc = _finish(doc, args.json, f"lab up: tier {args.tier}, lab {args.name}")
    if doc["ok"] and not args.json:
        tier = "" if args.tier == "core" else f" --tier {args.tier}"
        print(f"State: {doc['state_dir']}\nKubeconfig: {doc['kubeconfig']}\nNext: ./lab check{tier} --name {args.name}")
    return rc


def _run_check(root, args) -> int:
    from labctl import check

    doc = _state_guard("check", lambda: check.run_check(root, args.tier, name=args.name, log=_progress(args),
                                                        only=args.only or None))
    only = f" (only {', '.join(args.only)})" if args.only else ""
    return _finish(doc, args.json, f"lab check: tier {args.tier}{only}, lab {args.name}")


def _run_e2e(root, args) -> int:
    from labctl import e2e_command

    doc = _state_guard("e2e", lambda: e2e_command.run(root, name=args.name, https_port=args.https_port,
                                                      mirror_port=args.registry_port, mirror=not args.skip_mirror,
                                                      down_after=args.down, log=_progress(args)))
    return _finish(doc, args.json, f"lab e2e: lab {args.name}")


def _run_down(root, args) -> int:
    from labctl import down

    doc = _state_guard("down", lambda: down.run_down(root, name=args.name, purge=args.purge))
    return _finish(doc, args.json, f"lab down: lab {args.name}" + (" (purge)" if args.purge else ""))


def _run_accept(root, args) -> int:
    from labctl import accept, state
    from labctl.output import FAIL, check

    try:
        doc = accept.run_accept(root, name=args.name, profile=args.profile, tier=args.tier, products=args.product,
                                runner=args.runner, env=args.env, do_update_ledger=args.update_ledger,
                                keep=args.keep, log=_progress(args))
    except accept.AcceptUsageError as exc:
        sys.stderr.write(f"lab accept: {exc}\n")
        return EXIT_USAGE
    except (accept.AcceptError, state.StateError) as exc:
        doc = {"command": "accept", "ok": False,
               "checks": [check("accept", FAIL, str(exc), getattr(exc, "advice", "") or "")]}
        return _finish(doc, args.json, f"lab accept: profile {args.profile}, lab {args.name}")
    rc = _finish(doc, args.json, f"lab accept: {doc['profile']}/{doc['tier']}, run {doc['run_id']} "
                                 f"(strict, runner {args.runner}, lab {args.name})")
    if not args.json:
        for note in doc["notes"]:
            print(f"Note: {note}")
        print(f"Report: {doc['report']} and {doc['report_md']}")
        led = doc.get("ledger")
        if led:
            if led.get("refused"):
                print(f"Ledger {led['path']} NOT updated: {led['refused']}")
                return rc
            print(f"Ledger {led['path']}: {led['cells_updated']} cell(s) updated")
            print(led["diff"] or "(no cell changed)", end="" if led["diff"].endswith("\n") else "\n")
            if led["cells_updated"]:
                print("Review this diff and commit it with a message that names the capability change behind each "
                      "cell; lab accept never commits.")
    return rc


HANDLERS: dict = {
    "doctor": _run_doctor,
    "render": _run_render,
    "l0": _run_l0,
    "mirror": _run_mirror,
    "build": _run_build,
    "up": _run_up,
    "down": _run_down,
    "check": _run_check,
    "e2e": _run_e2e,
    "accept": _run_accept,
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help(sys.stderr)
        return EXIT_USAGE
    handler: Optional[Callable] = HANDLERS.get(args.command)
    if handler is None:  # pragma: no cover - argparse rejects unknown commands first
        parser.error(f"unknown command {args.command}")
    return handler(config.find_root(args.root), args)
