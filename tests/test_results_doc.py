"""Tests for experiments/_results_doc.py's replace_section().

Direct, focused unit tests against a tmp_path file (never the real
docs/results.md) -- same style as tests/test_run_comparison.py's
test_slug_distinguishes_near_collision_labels: this is pure string/file
logic with no broker or process spawning involved, so a real integration
test isn't needed to exercise it properly.

replace_section() backs three independent writers into the same shared
docs/results.md (experiments/run_comparison.py's two sections,
experiments/leaderless_boundary_case_demo.py's one, and
experiments/mq_lag_demo.py's one) -- the property that matters most here
is that replacing one marker's section never disturbs another marker's
section already in the file, which is what most of these tests are
actually checking for, directly or as a side effect.
"""

from __future__ import annotations

from experiments._results_doc import replace_section


def test_replace_section_creates_file_from_nothing(tmp_path):
    path = tmp_path / "results.md"
    assert not path.exists()

    replace_section(path, "header", "# Title\n\nSome content.")

    text = path.read_text()
    assert text == "<!-- BEGIN:header -->\n# Title\n\nSome content.\n<!-- END:header -->\n"


def test_replace_section_appends_new_marker_to_existing_file(tmp_path):
    path = tmp_path / "results.md"
    replace_section(path, "header", "# Title")

    replace_section(path, "main-sweep", "## Main sweep\n\nSome numbers.")

    text = path.read_text()
    # The first section is still there, untouched...
    assert "<!-- BEGIN:header -->\n# Title\n<!-- END:header -->" in text
    # ...and the new one was appended after it, not overwriting it.
    assert "<!-- BEGIN:main-sweep -->\n## Main sweep\n\nSome numbers.\n<!-- END:main-sweep -->" in text
    assert text.index("BEGIN:header") < text.index("BEGIN:main-sweep")


def test_replace_section_replaces_existing_marker_content(tmp_path):
    path = tmp_path / "results.md"
    replace_section(path, "main-sweep", "## Main sweep\n\nold numbers, run 1.")

    replace_section(path, "main-sweep", "## Main sweep\n\nnew numbers, run 2.")

    text = path.read_text()
    assert "new numbers, run 2." in text
    assert "old numbers, run 1." not in text
    # Exactly one BEGIN/END pair for this marker -- the old section's
    # content was replaced, not left behind alongside a second copy.
    assert text.count("<!-- BEGIN:main-sweep -->") == 1
    assert text.count("<!-- END:main-sweep -->") == 1


def test_replace_section_leaves_unrelated_marker_untouched(tmp_path):
    """The property that matters most: three independent scripts
    (run_comparison.py, leaderless_boundary_case_demo.py,
    mq_lag_demo.py) each own one or more sections of the same
    docs/results.md file -- replacing one marker's section must never
    disturb a different marker's section already in the file.
    """
    path = tmp_path / "results.md"
    replace_section(path, "header", "# Title")
    replace_section(path, "main-sweep", "## Main sweep\n\nnumbers A.")
    replace_section(path, "boundary-case-demo", "## Boundary case\n\nnumbers B.")

    before = path.read_text()
    boundary_section_before = before[
        before.index("<!-- BEGIN:boundary-case-demo -->") : before.index("<!-- END:boundary-case-demo -->")
        + len("<!-- END:boundary-case-demo -->")
    ]

    # Only main-sweep is replaced here.
    replace_section(path, "main-sweep", "## Main sweep\n\ndifferent numbers, later run.")

    after = path.read_text()
    boundary_section_after = after[
        after.index("<!-- BEGIN:boundary-case-demo -->") : after.index("<!-- END:boundary-case-demo -->")
        + len("<!-- END:boundary-case-demo -->")
    ]

    # The section this call never touched is byte-for-byte identical...
    assert boundary_section_after == boundary_section_before
    # ...while the one it did touch actually changed.
    assert "different numbers, later run." in after
    assert "numbers A." not in after
