#!/bin/bash
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# The Spark pod's entry (`lab-spark-submit`, spark tier): forge's
# spark-submit argv, unchanged, run under the image's own spark-submit with a
# local master.
#
#   entry.sh --name run --deploy-mode client --properties-file P --conf k=v app.py --config job.json
#
# The image is the apache/spark based spark-iceberg image: its spark-submit is
# /opt/spark/bin/spark-submit and its base conf directory /opt/spark/conf, which
# the image does not ship (nothing to copy).
#
# Only the options before the application are looked at:
# - `--master X` is dropped: the master is `local[2]` here;
# - `--properties-file P` becomes the pod's own properties file: the image's
#   base spark-defaults.conf (none in the image), then the copy of
#   P mounted from ConfigMap lab-spark (/spark/<basename of P>;
#   /spark/spark-defaults.conf when forge gave none), then
#   the OAuth2 credential from the environment (Secret
#   spark-lakekeeper-client). The credential is written to a 0600 file in this
#   pod only: never to argv, the ConfigMap or a log, and `exec` leaves no
#   shell holding it. Spark's own redaction covers the key (spark-defaults.conf).
# - `--conf` of `spark.sql.catalog.fluid*` or `spark.redaction.regex` is refused (exit 65): those keys
#   are the lab's (the catalog and what is hidden in logs). forge's allowlist already denies them
#   (fluid_build/build_runners/spark/_desired.py, `spark.sql.catalog`, `spark.redaction.`), so this is
#   defence in depth against a changed forge or a hand-made argv.
# Everything from the application on is passed through byte for byte.
#
# Test seams: LAB_SPARK_FS_ROOT (a prefix for the image paths above), LAB_SPARK_SUBMIT, LAB_SPARK_BASE_CONF,
# LAB_SPARK_MOUNT, LAB_SPARK_CONF_DIR.
set -euo pipefail
umask 077
mount="${LAB_SPARK_MOUNT:-/spark}"
conf="${LAB_SPARK_CONF_DIR:-/tmp/spark-conf}"
root="${LAB_SPARK_FS_ROOT:-}"

submit="${LAB_SPARK_SUBMIT:-$root/opt/spark/bin/spark-submit}"
base_conf="${LAB_SPARK_BASE_CONF:-$root/opt/spark/conf}"

# Refuse a --conf KEY=VALUE whose key belongs to the lab.
guard() {
  case "${1%%=*}" in
    spark.sql.catalog.fluid|spark.sql.catalog.fluid.*|spark.redaction.regex)
      echo "lab-spark-entry: --conf ${1%%=*} is the lab's and is refused" >&2
      exit 65 ;;
  esac
}

given=""
rest=()
while (($#)); do
  case "$1" in
    --master) shift; if (($#)); then shift; fi ;;
    --master=*) shift ;;
    --properties-file) given="${2:?--properties-file needs a path}"; shift 2 ;;
    --properties-file=*) given="${1#*=}"; shift ;;
    -v|--verbose|--supervise|-h|--help|--version) rest+=("$1"); shift ;;
    --conf=*) guard "${1#*=}"; rest+=("$1"); shift ;;
    --conf|-c) guard "${2:?$1 needs a value}"; rest+=("$1" "$2"); shift 2 ;;
    --*=*) rest+=("$1"); shift ;;
    -*) rest+=("$1" "${2:?$1 needs a value}"); shift 2 ;;
    *) break ;;               # the application resource: the rest is forge's, untouched
  esac
done

copy="$mount/$(basename "${given:-spark-defaults.conf}")"
if [ ! -f "$copy" ]; then
  echo "lab-spark-entry: $copy is not shipped in ConfigMap lab-spark (forge asked for ${given:-the default})" >&2
  exit 64
fi
mkdir -p "$conf"
# The apache/spark image ships no conf directory at all (seen live 2026-11-22).
if [ -d "$base_conf" ]; then cp -r "$base_conf/." "$conf/"; fi
props="$conf/spark-defaults.conf"
{
  echo
  echo "# fluid-fabric-lab (spark tier): $(basename "$copy")"
  cat "$copy"
  printf 'spark.sql.catalog.fluid.credential %s:%s\n' "${LAKEKEEPER_CLIENT_ID:?}" "${LAKEKEEPER_CLIENT_SECRET:?}"
} >> "$props"
export SPARK_CONF_DIR="$conf"
exec "$submit" --master 'local[2]' --properties-file "$props" ${rest[@]+"${rest[@]}"} "$@"
