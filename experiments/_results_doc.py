"""Shared marker-delimited section writer for docs/results.md.

docs/results.md is written by more than one script --
experiments/run_comparison.py (the main sweep table) and
experiments/leaderless_boundary_case_demo.py (the boundary-case demo
section) -- so neither can safely do a full-file overwrite: that would
silently destroy whatever the other script last wrote. Each instead owns
one HTML-comment-delimited section (`<!-- BEGIN:name -->` /
`<!-- END:name -->`) and replaces only the content between its own
markers via replace_section() below, leaving the rest of the file --
including the other script's section -- untouched.

docs/results.md itself is fully auto-generated and mechanical by design
(see docs/AUDIT_FINDINGS.md's §6): every section written into it is
tables, raw numbers, and a run timestamp -- never hand-authored prose
explaining *why* the numbers look the way they do. Hardcoding that kind
of analysis into a regenerated file is exactly what caused it to
silently drift out of sync with reality across past runs (see git
history: run_comparison.py's old _KNOWN_CHARACTERISTICS_NOTES, removed
-- by the time it was removed, it still cited ack_required=0 figures
from *before* a later fix changed them, because nothing forced it to be
touched again). That interpretation now lives in the top-level
README.md's "Results" section instead: hand-maintained, and referencing
this file and the raw JSONL logs under experiments/output/ rather than
duplicating numbers that can drift out of sync with them.
"""

from __future__ import annotations

from pathlib import Path

RESULTS_HEADER_MARKER = "header"

RESULTS_HEADER = (
    "# Replication strategy comparison results\n"
    "\n"
    "> **Auto-generated -- do not hand-edit.** This file is rebuilt by "
    "three scripts, each owning one or more sections marked below: "
    "`experiments/run_comparison.py` (main sweep and message-queue "
    "sweep), `experiments/leaderless_boundary_case_demo.py` "
    "(boundary-case demo), and `experiments/mq_lag_demo.py` "
    "(message-queue lag demo). Manual edits inside any marked section "
    "are overwritten the next time that section's script runs. For the "
    "hand-maintained interpretation of these numbers -- why they look "
    "the way they do -- see the top-level `README.md`'s \"Results\" "
    "section."
)


def replace_section(path: Path, marker: str, content: str) -> None:
    """Replace the content between `<!-- BEGIN:{marker} -->` and
    `<!-- END:{marker} -->` in the file at `path` with `content`,
    leaving everything else in the file -- including any other marked
    section a different script owns -- untouched.

    Creates the file if it doesn't exist yet, and creates the markers
    themselves (appended after whatever's already in the file, if
    anything) if this particular `marker` isn't present yet -- so the
    first script to ever run against a fresh file builds it up from
    nothing, and later scripts/markers just get appended after, in
    whatever order they first ran in.
    """
    begin = f"<!-- BEGIN:{marker} -->"
    end = f"<!-- END:{marker} -->"
    section = f"{begin}\n{content.strip()}\n{end}"

    text = path.read_text() if path.exists() else ""

    start_idx = text.find(begin)
    end_idx = text.find(end)
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        new_text = text[:start_idx] + section + text[end_idx + len(end):]
    elif text.strip():
        new_text = text.rstrip("\n") + "\n\n" + section + "\n"
    else:
        new_text = section + "\n"

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(new_text)
