# AI Usage Notes

Optional bonus per the spec. Documenting tools used, what was verified
manually, what an agent got wrong, and the guardrails that caught it.

## Tools used

- An AI coding assistant (Claude) for module scaffolding, docstring
  prose, test plan ideation, and reviewing the commit/resume invariants.
- Local Python interpreter (3.11) for fast smoke tests of every module
  as it landed — the smoke tests are the single most important
  verification step in this project.
- `psycopg.sql.Composed.as_string` to validate the rendered incremental
  query without a live DB connection.

## Workflow

I broke the build into ten "slices" (DB contract → domain types → utils
→ source → checkpoint store → run_id/fingerprint → writers →
staging/commit → orchestrator/API → tests/README). After every slice I
ran a focused smoke test that exercised that slice's contracts before
moving on. The smoke tests evolved into the unit tests in `tests/unit/`.

This was the most important guardrail: each slice was *independently
testable* by design, so an agent suggestion that subtly broke
determinism (one example below) failed the smoke test in the slice it
was introduced, not three slices later.

## Things I verified manually

- **The composite watermark predicate matches the spec verbatim.** Read
  the SQL string emitted by `build_delta_query()` against the spec's
  required form character-by-character. Verified `AND` binds tighter
  than `OR` so the parens around the second clause are redundant but
  documentary.
- **Datetime serialization is byte-stable.** Wrote a unit test that
  compares two stagings of the same input across different staging
  dirs and asserts byte equality. Caught a Pydantic v2 quirk (see
  below) before it could ship.
- **Crash-resume idempotency.** Walked through the commit plan
  step-by-step and asked "if we crash here, what does resume see?" for
  every step. Wrote a unit test for each crash point.
- **Partition tie behavior.** Computed in Python the expected
  `updated_at` values for the seed and verified `case_id i` and
  `case_id i+100` share a timestamp for `i in 1..100`. The integration
  test for composite watermark uses this knowledge to land a checkpoint
  in the middle of a tied pair and verify the next run consumes the
  right rows.

## Edge cases I checked

- Naive datetimes get rejected at serialization time (catches a real
  bug class).
- A checkpoint file that exists but is unparseable raises
  `CheckpointCorruptError` rather than silently re-ingesting from zero.
- The lake's read-existing-bytes step preserves prior content
  byte-for-byte (verified by writing a known prior file and asserting
  the merged file starts with that exact byte sequence).
- Zero-delta runs produce a deterministic, well-defined `run_id` and
  do not modify the share file.
- A run dir with `READY` but no commit plan would be a corrupt journal
  state — the resume path raises rather than silently drop the run.

## What an agent got wrong, and how I caught it

### The Pydantic v2 nested-datetime trap

An early draft of `ShareWriter` used the `ShareRecord` Pydantic model
with `model_dump(mode="json")`. The model's `field_serializer` converts
the *top-level* `updated_at` to the canonical `Z`-suffix form, but
`model_dump` does **not** apply that serializer to datetimes inside the
`record: dict[str, Any]` field — those got Pydantic's default
`"2026-04-15T12:00:00+00:00"` form.

The resulting share record had `Z`-suffix at the top level and
`+00:00` inside `record.updated_at`. Both representations are valid
JSON, but byte-for-byte determinism — which the spec demands for the
share artifact — was silently broken across the boundary.

The smoke test that compared two stagings of the same input via
`bytes equality` caught this immediately. The fix was to bypass
`model_dump` for the share writer and hand-build the record dict,
routing every datetime through `iso_utc_z()` once. The Pydantic
`ShareRecord` model is still useful as a type contract; it just isn't
on the hot serialization path. I also updated `EventWriter` to use a
`Protocol` type instead of importing the Pydantic `Event` model
directly, so the writer module loads in environments without Pydantic
(useful for fast unit tests and for keeping the module's dependency
graph honest).

### The lake duplication risk on resume

The first design draft for the lake writer used `open(... "a")` —
straightforward append. The agent suggested it; I asked "what happens
on resume after a crash between rename and checkpoint?" and walked
through the failure: the live partition has the new rows, the staged
copy is gone, the checkpoint is unchanged, the next run re-computes the
same delta and would `read existing + append` — adding the new rows
*again*.

The fix that made the rest of the system simple: implement the "append"
as `read-existing-bytes + concat-new + stage as full file`, then atomic
rename. This makes lake commits idempotent under retry because the
staged content is byte-identical to the desired final state. Resume
just plays the rename forward; it never re-merges. The unit test
`test_stage_merged_preserves_existing_bytes_verbatim` is the
regression for this.

## Commands I ran

```bash
# Smoke tests after each slice
PYTHONPATH=src python3 - <<'PY' ... PY

# Seed math verification (Python recreation of the SQL interval arithmetic)
python3 -c "from datetime import ...; ..."

# SQL composition rendering
python3 -c "from caseware_sync.source.repository import build_delta_query; ..."
```

The full smoke-test transcripts informed the unit tests in
`tests/unit/`; nothing in this project ships without first being
exercised against an actual byte-comparison.
