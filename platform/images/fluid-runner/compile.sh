#!/bin/sh
# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
#
# Regenerate requirements.txt and requirements-build.txt (every distribution
# pinned by version and sha256, for every platform). Run it after any edit to
# requirements.in, requirements-build.in or forge's pin in versions.lock.yaml.
#
#   compile.sh [--allow-unmerged]
#
# Needs uv, yq (labctl reads the lock with it), gh, curl, python3 and the
# network. forge's pin is read from the lock, the same place `lab build` reads
# it, so it is not typed here. What it does with the pin:
#   - for a release pin (release: true), requires tag v<version> to resolve to
#     the pinned commit (an annotated tag is peeled);
#   - otherwise refuses a commit that is not in forge-cli main's history (an unmerged
#     pin needs --allow-unmerged, and a reviewer's eyes);
#   - downloads the archive itself, checks its sha256 against the lock, and
#     gives uv that verified local file, never the URL;
#   - writes the pin as requirements.txt's first line, which
#     tests/launcher/test_build.py compares with the lock.
set -eu
allow_unmerged=0
case "${1:-}" in
  "") ;;
  --allow-unmerged) allow_unmerged=1 ;;
  *) echo "usage: compile.sh [--allow-unmerged]" >&2; exit 2 ;;
esac
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
pin() { (cd "$root" && python3 -m labctl.build forge "$1"); }
commit="$(pin commit)"; sha256="$(pin sha256)"; url="$(pin url)"; header="$(pin header)"
SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DATA_PRODUCT_FORGE="$(pin local_version)"
export SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DATA_PRODUCT_FORGE

if [ "$(pin release)" = true ]; then
  # A release pin must be the release tag's commit, not merely a commit in main.
  tag="v$(pin version)"
  ref="$(gh api "repos/Agenticstiger/forge-cli/git/ref/tags/${tag}" --jq '.object.type + " " + .object.sha')"
  if [ "${ref%% *}" = tag ]; then   # an annotated tag: peel it to its commit
    tagged="$(gh api "repos/Agenticstiger/forge-cli/git/tags/${ref#* }" --jq .object.sha)"
  else
    tagged="${ref#* }"
  fi
  if [ "$tagged" != "$commit" ]; then
    echo "compile.sh: release pin: tag $tag is commit $tagged, the lock says $commit" >&2
    exit 1
  fi
  status=identical   # the tag's commit; main ancestry does not apply
else
  status="$(gh api "repos/Agenticstiger/forge-cli/compare/${commit}...main" --jq .status)"
fi
case "$status" in
  identical|ahead) ;;
  *) if [ "$allow_unmerged" = 1 ]; then
       echo "compile.sh: forge commit $commit is not in main's history (compare status: $status); --allow-unmerged given" >&2
     else
       echo "compile.sh: forge commit $commit is not in forge-cli main's history (compare status: $status). Pin a merged commit, or pass --allow-unmerged." >&2
       exit 1
     fi ;;
esac

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
curl -sSfL -o "$work/forge.tar.gz" "$url"
got="$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$work/forge.tar.gz")"
if [ "$got" != "$sha256" ]; then
  echo "compile.sh: the archive at $url has sha256 $got, the lock says $sha256" >&2
  exit 1
fi
# uv reads the verified file. The copy keeps the name requirements.in, so the "via" notes in the output are unchanged.
python3 - "$here/requirements.in" "$work/requirements.in" "$url" "$work/forge.tar.gz" <<'PY'
import sys
src, dst, url, local = sys.argv[1:5]
text = open(src, encoding="utf-8").read()
if text.count(url) != 1:
    sys.exit(f"compile.sh: requirements.in must name the lock's forge archive {url} exactly once")
open(dst, "w", encoding="utf-8").write(text.replace(url, "file://" + local))
PY

cd "$work"
# The existing file steers uv to the versions already pinned, so a recompile changes only what the edit needs.
cp "$here/requirements.txt" requirements.txt.new
uv pip compile requirements.in --generate-hashes --universal --python-version 3.13 \
  --no-emit-package data-product-forge --custom-compile-command "platform/images/fluid-runner/compile.sh" \
  -o requirements.txt.new > /dev/null
{ printf '%s\n' "$header"; cat requirements.txt.new; } > "$here/requirements.txt"
cd "$here"
uv pip compile requirements-build.in --generate-hashes --universal --python-version 3.13 \
  --custom-compile-command "platform/images/fluid-runner/compile.sh" \
  -o requirements-build.txt > /dev/null
