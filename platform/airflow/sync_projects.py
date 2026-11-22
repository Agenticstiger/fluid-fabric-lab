# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""Keep s3://lab-airflow/projects/ mirrored into FLUID_PROJECT_DIR on the scheduler pod.

forge's generated DAG runs `fluid apply "$FLUID_PROJECT_DIR/<contract path>"`,
so the scheduler needs the product checkout the DAG names. CI uploads it to the
`projects/` prefix next to `dags/`, and this sidecar (same image, same Airflow connection
`lab_s3`, read-only identity) copies it to an emptyDir the scheduler container
mounts. It is a loop of full listings: the bucket holds a handful of files.

Why not a second S3DagBundle: a bundle is materialised only for the DAGs that
belong to it, so the scheduler would never have the files, and the dag
processor would parse the project's Python files as DAGs.

Standard library plus boto3, which the Airflow image already has. Written so a
failed pass leaves the last good copy in place: files are downloaded to a
temporary name and renamed, and a pass that cannot list the bucket deletes nothing.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import time

KEEP = ".lab-keep"      # the marker `lab up` puts under every prefix so the prefix exists


def target_for(dest: pathlib.Path, rel: str) -> pathlib.Path:
    """`dest/rel`, refusing a key that would leave `dest` (`..` or an absolute path)."""
    root = dest.resolve()
    path = (root / rel).resolve()
    if rel.startswith("/") or root != path and root not in path.parents:
        raise ValueError(f"object key {rel!r} leaves {dest}")
    return path


def sync(s3, bucket: str, prefix: str, dest: pathlib.Path, seen: dict, first: bool = False) -> dict:
    """One pass. `seen` maps a relative path to the ETag last downloaded.

    Returns {"downloaded", "deleted", "skipped"}. A key that cannot be mirrored (it climbs out of `dest`, or a
    path is both a file and a directory) is logged and skipped: it never stops the pass or its deletes. On the
    first pass of a process `seen` knows nothing, so files an earlier run left that S3 no longer lists are removed
    too (`*.part` leftovers included).
    """
    listing = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            rel = obj["Key"][len(prefix):]
            if rel and not rel.endswith("/") and pathlib.PurePosixPath(rel).name != KEEP:
                listing[rel] = obj.get("ETag", "")
    downloaded = deleted = 0
    skipped = []
    for rel, etag in sorted(listing.items()):
        try:
            path = target_for(dest, rel)
            if seen.get(rel) == etag and path.is_file():
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            part = path.with_name(path.name + ".part")
            s3.download_file(bucket, prefix + rel, str(part))
            os.replace(part, path)
        except (OSError, ValueError) as exc:
            skipped.append(rel)
            print(f"skipped s3://{bucket}/{prefix}{rel}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            continue
        seen[rel] = etag
        downloaded += 1
    wanted = {rel for rel in listing if rel not in skipped}
    for rel in [r for r in seen if r not in wanted]:
        target_for(dest, rel).unlink(missing_ok=True)
        del seen[rel]
        deleted += 1
    if first:
        for found in sorted(dest.rglob("*")):
            if found.is_file() and found.relative_to(dest).as_posix() not in wanted:
                found.unlink()
                deleted += 1
    return {"downloaded": downloaded, "deleted": deleted, "skipped": len(skipped)}


def client_from_connection(raw: str):
    """A boto3 S3 client from the Airflow connection JSON (AIRFLOW_CONN_LAB_S3), path-style."""
    import boto3
    from botocore.config import Config

    conn = json.loads(raw)
    extra = conn.get("extra") or {}
    return boto3.client(
        "s3", endpoint_url=extra.get("endpoint_url"), region_name=extra.get("region_name", "us-east-1"),
        aws_access_key_id=conn["login"], aws_secret_access_key=conn["password"], verify=extra.get("verify", True),
        config=Config(s3={"addressing_style": "path"}),
    )


def main() -> int:
    bucket, prefix = os.environ.get("SYNC_BUCKET", "lab-airflow"), os.environ.get("SYNC_PREFIX", "projects/")
    dest = pathlib.Path(os.environ["FLUID_PROJECT_DIR"])
    every = float(os.environ.get("SYNC_INTERVAL_S", "30"))
    dest.mkdir(parents=True, exist_ok=True)
    s3, seen, first = client_from_connection(os.environ["AIRFLOW_CONN_LAB_S3"]), {}, True
    while True:
        try:
            done = sync(s3, bucket, prefix, dest, seen, first)
            first = False
            if done["downloaded"] or done["deleted"]:
                print(f"synced s3://{bucket}/{prefix}: {done['downloaded']} downloaded, {done['deleted']} deleted",
                      flush=True)
        except Exception as exc:  # noqa: BLE001 - keep the last good copy and try again
            print(f"sync of s3://{bucket}/{prefix} failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        time.sleep(every)


if __name__ == "__main__":
    sys.exit(main())
