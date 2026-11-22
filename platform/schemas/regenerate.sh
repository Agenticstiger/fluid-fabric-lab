#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# Regenerates the vendored kubeconform schemas from the pins in
# versions.lock.yaml (the single source of truth). Maintainer tool, not an L0
# check: L0 only runs `regenerate.sh verify` (offline) and kubeconform against
# the committed files.
#
#   regenerate.sh crds    offline: render every CRD from platform/charts and
#                         convert it with tools/crd2jsonschema.py
#   regenerate.sh core    NETWORK: download the kinds in kubernetes/kinds.txt
#                         from yannh/kubernetes-json-schema at the pinned commit
#   regenerate.sh sums    rewrite SHA256SUMS over everything vendored
#   regenerate.sh verify  offline: check SHA256SUMS
#
# Requires the pinned helm, yq and python3 (>=3.10). Exits non-zero on any
# failure; a kind with no upstream schema is listed in kubernetes/MISSING.txt
# and makes `core` exit 3 unless it is in the reviewed exception list below.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
lock="$root/versions.lock.yaml"
charts="$root/platform/charts"

# Kinds that upstream does not publish a schema for. Reviewed: kubeconform
# cannot validate them, so the schema check must name them explicitly
# (-skip) and the CRD files are instead pinned by chart digest.
REVIEWED_MISSING="apiextensions.k8s.io/v1 CustomResourceDefinition"

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "regenerate.sh: $1 not on PATH" >&2; exit 2; }; }
lockq() { yq -e "$1" "$lock"; }

cmd_crds() {
  need helm; need yq; need python3
  local out="$here/crds" tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf -- "$tmp"' RETURN
  : > "$tmp/crds.yaml"
  # Every chart with crd_render_args in versions.lock.yaml contributes CRDs.
  local n i file args
  n="$(lockq '[.charts[] | select(has("crd_render_args"))] | length')"
  for ((i = 0; i < n; i++)); do
    file="$(lockq "[.charts[] | select(has(\"crd_render_args\"))][$i].file")"
    args="$(lockq "[.charts[] | select(has(\"crd_render_args\"))][$i].crd_render_args | join(\" \")" || true)"
    # shellcheck disable=SC2086 # args is a reviewed, space-separated flag list
    helm template crds "$root/$file" --namespace crds --include-crds $args >> "$tmp/crds.yaml"
    printf '\n---\n' >> "$tmp/crds.yaml"
  done
  yq -o=json -I=0 'select(.kind == "CustomResourceDefinition")' "$tmp/crds.yaml" > "$tmp/crds.jsonl"
  # Regenerate into a clean directory so removed CRDs do not linger.
  find "$out" -name '*.json' -type f -delete 2>/dev/null || true
  python3 -I "$here/tools/crd2jsonschema.py" "$out" < "$tmp/crds.jsonl"
}

kind_file() {  # "<apiVersion> <Kind>" -> yannh file name
  local api="$1" kind="$2" group version
  kind="$(printf '%s' "$kind" | tr '[:upper:]' '[:lower:]')"
  if [[ "$api" == */* ]]; then
    group="${api%%/*}"; version="${api##*/}"
    printf '%s-%s-%s.json' "$kind" "${group%%.*}" "$version"
  else
    printf '%s-%s.json' "$kind" "$api"
  fi
}

cmd_core() {
  need curl; need yq
  local commit kver flavour out base missing=0 api kind f
  commit="$(lockq '.schemas.kubernetes.commit')"
  kver="$(lockq '.schemas.kubernetes.version')"
  flavour="$(lockq '.schemas.kubernetes.flavour')"
  out="$here/kubernetes/${kver}-${flavour}"
  base="https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${commit}/${kver}-${flavour}"
  mkdir -p "$out"
  : > "$here/kubernetes/MISSING.txt"
  while read -r api kind; do
    [[ -z "${api:-}" || "$api" == \#* ]] && continue
    f="$(kind_file "$api" "$kind")"
    if ! curl -fsSL --retry 3 -o "$out/$f" "$base/$f"; then
      rm -f -- "$out/$f"
      echo "$api $kind ($f)" >> "$here/kubernetes/MISSING.txt"
      if [[ " $REVIEWED_MISSING " != *" $api $kind "* ]]; then missing=1; fi
    fi
  done < "$here/kubernetes/kinds.txt"
  curl -fsSL -o "$here/kubernetes/LICENSE" \
    "https://raw.githubusercontent.com/yannh/kubernetes-json-schema/${commit}/LICENSE"
  if (( missing )); then
    echo "regenerate.sh: unreviewed kinds without an upstream schema, see kubernetes/MISSING.txt" >&2
    exit 3
  fi
}

cmd_sums() {
  (cd "$here" && find crds kubernetes -type f -name '*.json' | LC_ALL=C sort | while read -r f; do sha256 "$f"; done) > "$here/SHA256SUMS"
}

cmd_verify() {
  [[ -s "$here/SHA256SUMS" ]] || { echo "regenerate.sh: SHA256SUMS missing or empty" >&2; exit 1; }
  (cd "$here" && sha256 -c --quiet SHA256SUMS)
  # Every vendored schema must be listed: no unchecked files.
  local listed actual
  listed="$(awk '{print $2}' "$here/SHA256SUMS" | LC_ALL=C sort)"
  actual="$(cd "$here" && find crds kubernetes -type f -name '*.json' | LC_ALL=C sort)"
  if [[ "$listed" != "$actual" ]]; then
    echo "regenerate.sh: schema files and SHA256SUMS differ:" >&2
    diff <(printf '%s\n' "$listed") <(printf '%s\n' "$actual") >&2 || true
    exit 1
  fi
}

case "${1:-}" in
  crds) cmd_crds ;;
  core) cmd_core ;;
  sums) cmd_sums ;;
  verify) cmd_verify ;;
  *) echo "usage: regenerate.sh crds|core|sums|verify" >&2; exit 2 ;;
esac
