# SPDX-FileCopyrightText: 2026 Agentics Transformation Limited
# SPDX-License-Identifier: Apache-2.0
"""L0 gate `brand_fence`: third-party names appear only where allowlisted.

Scans every file under the root (not .git) and every file and directory path
for the [terms] in tools/l0/brand-fence.conf. A hit fails unless the line
carries `brand-fence: allow`, sits inside an allow-begin/allow-end block, or
the file matches an [allow-paths] glob (for that term).

Matching is deliberately loose, because a fence that only sees the exact
spelling is easy to walk round:
- case-insensitive SUBSTRING match, so joined, plural, camel-case and
  suffixed forms count;
- an optional dash, dot, underscore or bullet between letters;
- text is NFKC-normalised, invisible format characters (zero-width space,
  joiners, bidi marks) are dropped and lookalike Cyrillic/Greek letters are
  folded to Latin first;
- base64 tokens are decoded and matched too;
- a file with NUL bytes or in UTF-16 is still read as text; only known binary
  formats (gzip, zip, PNG, JPEG, PDF, ELF, xz, zstd, compiled .pyc) are not
  text-scanned, and their paths still are;
- __pycache__ is walked: anything there that is not a .pyc fails.

  brand_fence.py [--root DIR] [--config FILE]

The config always comes from this repository unless --config is given, so a
fixture tree is judged by the real allowlist.
"""

from __future__ import annotations

import argparse
import base64
import os
import re
import unicodedata
from pathlib import Path

import sys

sys.dont_write_bytecode = True  # keep the checkout free of __pycache__ (set before local imports)
import _common as c  # noqa: E402

LINE_ALLOW = "brand-fence: allow"
BEGIN, END = "brand-fence: allow-begin", "brand-fence: allow-end"
# A block marker is a line of its own: optional comment syntax around the marker.
_MARK = r"^\s*(?:#|//|;|--|<!--|/?\*+)?\s*{}\s*(?:-->|\*/)?\s*$"
BEGIN_RE, END_RE = re.compile(_MARK.format(re.escape(BEGIN))), re.compile(_MARK.format(re.escape(END)))


# Lookalike letters a name could be spelled with (Cyrillic, Greek, others) ->
# the Latin letter. NFKC folds full-width and styled forms first.
CONFUSABLES = str.maketrans({
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x", "к": "k", "м": "m", "т": "t",
    "н": "h", "в": "b", "і": "i", "ј": "j", "ѕ": "s", "ԁ": "d", "ӏ": "l", "ɑ": "a", "ο": "o", "α": "a",
    "ε": "e", "ρ": "p", "ν": "v", "κ": "k", "τ": "t", "ι": "i", "υ": "u", "μ": "u", "ℓ": "l", "ı": "i",
    "А": "A", "Е": "E", "О": "O", "Р": "P", "С": "C", "К": "K", "М": "M", "Т": "T", "Н": "H", "В": "B",
    "І": "I", "Ј": "J", "Ѕ": "S", "Ο": "O", "Α": "A", "Ε": "E", "Ρ": "P", "Κ": "K", "Τ": "T", "Ι": "I",
})
# Separators someone could put between the letters of a name (a dash, a dot,
# an underscore, a bullet). Whitespace is not one: it would match ordinary prose.
SEP = r"[-_.\u00b7\u2010-\u2015\u2022*'`]?"
B64_TOKEN = re.compile(r"[A-Za-z0-9+/]{8,}={0,2}")
def normalise(text: str) -> str:
    """NFKC, drop invisible format characters (zero-width space, joiners, soft
    hyphen, bidi marks), and fold lookalike letters to Latin."""
    text = unicodedata.normalize("NFKC", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Cf")
    return text.translate(CONFUSABLES)


def b64_texts(line: str) -> list:
    """Printable text hidden in base64 tokens of a line."""
    out = []
    for tok in B64_TOKEN.findall(line):
        if len(tok) % 4 == 1:
            continue
        try:
            raw = base64.b64decode(tok + "=" * (-len(tok) % 4))
        except ValueError:
            continue
        txt = raw.decode("utf-8", "replace")
        if txt and sum(ch.isprintable() for ch in txt) / len(txt) > 0.95:
            out.append(txt)
    return out


read_text = c.read_text_any


def walk(root: Path):
    """Every file under root except .git. __pycache__ is walked too: only
    compiled .pyc files may live there (anything else fails)."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != ".git")
        for f in sorted(filenames):
            yield Path(dirpath) / f


def load_config(path: Path) -> tuple:
    if not path.is_file():
        raise c.GateError(f"brand fence config {path} not found")
    terms, allows, section = [], [], None
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            continue
        if section == "terms":
            if not re.fullmatch(r"[a-z0-9]+", line):
                raise c.GateError(f"{path.name}:{n}: a term is lowercase letters/digits only: {line!r}")
            terms.append(line)
        elif section == "allow-paths":
            parts = line.split()
            only = set(parts[1].split(",")) if len(parts) > 1 else None
            if len(parts) > 2:
                raise c.GateError(f"{path.name}:{n}: expected `<glob> [term,term]`")
            allows.append((parts[0], c.glob_to_regex(parts[0]), only))
        else:
            raise c.GateError(f"{path.name}:{n}: entry outside [terms]/[allow-paths]")
    if not terms:
        raise c.GateError(f"{path.name}: no [terms]")
    return terms, allows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=str(c.REPO))
    ap.add_argument("--config", default=str(c.REPO / "tools/l0/brand-fence.conf"))
    args = ap.parse_args()
    root = Path(args.root).resolve()
    terms, allows = load_config(Path(args.config).resolve())
    # Substring match (no word boundaries), letters optionally separated: a
    # joined, plural or camel-case form is still the name.
    term_re = re.compile("(" + "|".join(SEP.join(re.escape(ch) for ch in term)
                                        for term in sorted(terms, key=len, reverse=True)) + ")", re.I)
    rep = c.Report("brand_fence")

    def which_term(match: str) -> str:
        bare = re.sub(r"[^a-z0-9]", "", match.lower())
        return next((t for t in sorted(terms, key=len, reverse=True) if t == bare), bare)

    def path_allowed(relpath: str, term: str) -> bool:
        for _, rx, only in allows:
            if rx.match(relpath) and (only is None or term in only):
                return True
        return False

    def hits_in(text: str) -> list:
        norm = normalise(text)
        found = [m.group(1) for m in term_re.finditer(norm)]
        for hidden in b64_texts(text):
            found += [f"{m.group(1)} (base64)" for m in term_re.finditer(normalise(hidden))]
        return found

    scanned = binary = hits = allowed_files = 0
    for p in walk(root):
        r = c.rel(p, root)
        for name in hits_in(r):
            t = which_term(name.split(" ")[0])
            if not path_allowed(r, t):
                hits += 1
                rep.fail(f"{r}: path names {name!r} (not in an allowlisted path)")
        if "__pycache__" in Path(r).parts:
            if p.suffix != ".pyc":
                hits += 1
                rep.fail(f"{r}: only compiled .pyc files belong in __pycache__ (a file hidden here is not scanned)")
            binary += 1
            continue
        text, how = read_text(p)
        if text is None:
            binary += 1
            continue
        if any(rx.match(r) and only is None for _, rx, only in allows):
            allowed_files += 1
            continue  # every term is allowed in this file: nothing to check
        scanned += 1
        in_block = False
        for n, line in enumerate(text.splitlines(), 1):
            if BEGIN_RE.match(line):
                in_block = True
                continue
            if END_RE.match(line):
                in_block = False
                continue
            if in_block or LINE_ALLOW in line:
                continue
            for name in hits_in(line):
                t = which_term(name.split(" ")[0])
                if not path_allowed(r, t):
                    hits += 1
                    extra = "" if how == "text" else f" (read as {how})"
                    rep.fail(f"{r}:{n}: names {name!r}{extra}: {normalise(line).strip()[:140]}")
        if in_block:
            hits += 1
            rep.fail(f"{r}: `{BEGIN}` without a matching `{END}`")
    if scanned == 0 and allowed_files == 0:
        rep.fail("no text files found to scan")
    elif hits == 0:
        rep.ok(f"{scanned} text file(s) and every path: no third-party name outside the allowlist "
               f"({allowed_files} fully allowlisted file(s) and {binary} binary file(s) not text-scanned; "
               "their paths were checked)")
    return rep.finish()


if __name__ == "__main__":
    c.main_wrapper(main)
