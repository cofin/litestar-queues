=========================
Writing documentation
=========================

Write for the reader's next goal. Keep the beginner path short. Explain the
default before alternatives, and move backend internals or matrices to a
focused guide or reference page.

Authoring workflow
==================

1. Read the live implementation, public types, focused tests, and runnable
   example before writing a behavioral claim.
2. Choose one page mode: concept, how-to, reference, explanation, or gallery.
3. Start a how-to with a complete minimal example. Import long examples with
   ``literalinclude`` from ``examples/``.
4. Link to advanced options instead of repeating them across landing pages.
5. Preserve existing URLs. Rewrite landing pages in place or provide a
   compatibility page when content moves.
6. Run the deterministic audit, structural tests, docs build, and link check.

.. code-block:: bash

   make docs-audit
   uv run pytest src/tests/unit/test_docs_structure.py \
     src/tests/unit/examples/test_htmx_realtime_example.py
   make docs
   make docs-linkcheck

Page templates
==============

**Concept:** define the topic and its limits, show one compact flow, contrast
terms readers may confuse, then link to goal-oriented guides.

**How-to:** state the outcome, show a complete minimal example, explain only
the decisions in that example, then link to alternatives and operations.

**Reference:** list the public API, defaults, fields, return values, and errors.
Do not turn generated API output into a tutorial.

**Gallery:** identify the main runnable example, explain how each variant
differs, state its process layout, and link to browser test coverage.

Review rubric
=============

* The page has one audience, one goal, and one dominant mode.
* The default path appears before optional integrations and edge cases.
* Queue persistence, execution placement, worker wakeups, live task events,
  and event history remain separate concepts.
* Commands, defaults, install extras, statuses, and wire names match source.
* The README and quickstart use the same complete application.
* Every toctree and ``literalinclude`` target exists.
* No primary guide exceeds about 1,200 words without a reference or matrix reason.
* Cards, callouts, and tables remain readable in light and dark themes.

Audit output
============

``tools/docs_audit.py`` is read-only. It inventories pages and toctree
membership, counts words/headings/code, flags mixed API/tutorial pages,
reviews the primary Python block, reports vocabulary and obsolete terms, and
notes likely README/quickstart duplication. Review prompts require judgment;
missing structural targets fail the command.
