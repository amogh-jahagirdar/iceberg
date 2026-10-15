# V4: partition tuples -> content stats (session context)

Not for merge. Start a new session by pointing it at this file.

## Goal

Iceberg v4 removes partition tuples from manifest entries. Partition information must be
represented in `content_stats`. This work covers the **write direction for monotonic
(order-preserving) transforms only**: given a legacy v2/v3 entry (`DataFile`/`ManifestEntry`) with
a spec ID and a partition tuple, produce the v4 `TrackedFile` entry whose `content_stats` carries
the source-column bounds and counts implied by the partition tuple.

Out of scope for now: bucket (needs stats on expressions), the read direction, dropping the tuple.

## Base

Stacked on Steven's PR https://github.com/apache/iceberg/pull/16936 ("Core: V4 write direction
wrappers"), head `b1e4e30ab` at the time of writing (`pr-16936` locally, fetched with
`git fetch apache pull/16936/head:pr-16936`). `apache/main` has moved on since; rebase later.
That PR adds `TrackedFileAdapters.forDataFile(..)` / `DataTrackedFile` and `MapBackedContentStats`
(a reusable `ContentStats` view over a `ContentFile`'s metric maps).

## Stack (each branch is one commit on the previous; diff each against its parent)

| Branch | Abstraction | Commit |
|---|---|---|
| `v4-part2stats-1-source-bounds` | `StatsUtil.sourceBounds(schema, field, partitionValue)`: pure inversion from a partition value to inclusive source-column bounds. Tests: `TestStatsUtilSourceBounds`. | Core: Derive source column bounds from partition values |
| `v4-part2stats-2-derived-stats-view` | `MapBackedContentStats(schema, specsById)`: `wrap(file)` looks up the spec from `file.specId()` and overlays partition-implied stats. Tests: `TestMapBackedContentStatsFromPartition`. `forDataFile(schema, specsById)` plumbing. | Core: Reflect partition values in MapBackedContentStats |
| `v4-part2stats-3-adapter-stats` | `DataTrackedFile` reports content stats when the file has metrics OR partition-implied stats. Rejects unknown spec / bucket when wrapped. | Core: Report partition-derived content stats from DataTrackedFile |
| `v4-part2stats-4-context` | This file only. | |

Compare with e.g. `git diff v4-part2stats-1-source-bounds..v4-part2stats-2-derived-stats-view`.
The stack's base is `pr-16936` (pushed as `v4-part2stats-0-base-pr16936`).

## Design

Inversion (`StatsUtil.sourceBounds`, via the public `PartitionSpecVisitor`; no `api/` change):

| Transform | Source bounds for partition value p |
|---|---|
| identity | `[p, p]` (coerced for promoted source types; NaN -> no bounds) |
| truncate int/long/decimal(w) | `[p, p+w-1]`, upper saturates on overflow |
| truncate string/binary(w) | `[p, p]` if shorter than w, else `[p, incremented prefix]`; upper may be null |
| year/month/day/hour on date/timestamp/timestamp_ns | `[epoch + p units, epoch + (p+1) units - 1 tick]`, saturating |
| null partition value | no bounds (counts handled by the caller) |
| void | no bounds / skipped |
| bucket, unknown | throws `UnsupportedOperationException` (the write fails) |

Combining with a file's real stats, per partition source column of one entry (in
`MapBackedContentStats`):
- counts: the partition is authoritative. `value_count = recordCount`; `null_value_count` is
  `recordCount` for a null partition value, else 0. NaN count and total bytes stay file-only.
- bounds: intersection (`lower = max(file lower, derived lower)`, `upper = min(...)`), a missing
  side is unbounded. Both inputs are valid bounds so the intersection is valid and recoverable
  (transform(lower) == partition value) even when file bounds were truncated.
- disjoint file vs derived range (corrupt metadata): trust the partition (use the derived range).
- several partition fields on one source column are intersected with each other first.
- entries are single-spec: the spec comes from `specsById.get(file.specId())`. Void fields and
  source columns that are no longer in the table schema are skipped.
- `tight_bounds` is always false (unchanged from the PR's view).

The adapter state holds `specsById`; callers only pass files. The partition tuple is still
reported unchanged, so the change is additive.

## Decisions made

- Disjoint ranges: trust the partition. Bucket / any non-monotonic non-void transform: fail the write.
- Void: skip. Float NaN identity: no bounds.
- `MetricsConfig` (already on main, #17960) forces Full metrics for the current spec's
  order-preserving source columns. `MetricsConfig.forTable` describes the table as it is today and
  should NOT be changed for historical specs; that concern is limited to entries being converted.
- Spec (latest `apache/main` format/spec.md): the `content_stats` type is "determined by the
  writer", all field structs optional, and a v4 manifest may hold files of different specs. There is
  no "same stats type for every entry" requirement.
- The v4 writer should just pass entries through; stats complexity stays inside the adapter.
- Avoid the name "partition stats" (a different spec feature).

## Open items / next

1. Stats projection: `copyWithStats(Set<Integer>)` / `copyWithoutStats` and the reader's stats
   projection must keep partition-source stats whenever `partition()` is needed
   (`V4ManifestReader.idsToRestore`). The write wrappers' `copy*` throw today.
2. Read direction: rebuild the partition tuple from lower bounds (`transform.apply(lower)`) in
   `TrackedDataFile.partition()` / `asDataFile`; then stop writing the tuple.
3. Old-spec entries whose source column has no bounds field in the manifest's stats type
   (config `none`, dropped column): derived range has nowhere to go. Fill-only-if-present, or keep
   the tuple for those. Unresolved. A `statsWriteSchema(schema, config, spec)` overload was
   discussed and deliberately NOT built (no consumer yet; depends on how the v4 writer reads
   `ContentStats.type()`); ask Steven how his writer will consume the stats type.
4. Allocation: `MapBackedContentStats.wrap` allocates one small `PartitionStats` per partition field
   per file. The PR's goal is zero per-row allocation; revisit if benchmarks care.
5. Promoted source types: identity/truncate coerce Integer->Long and Float->Double in
   `sourceBounds`; other promotions (e.g. decimal precision) are untested.
6. Float identity: only NaN is handled; -0.0 vs 0.0 is not.
7. Nested/stats-ID-range edge cases: source IDs >= 999,950 have no stats struct and are silently
   ignored (`fieldStatsStruct` returns null).
8. Rebase on current `apache/main` once #16936 lands or moves.

## Environment

- Repo remote `origin` = the fork `amogh-jahagirdar/iceberg`; `apache` = upstream.
- No JDK on PATH by default; JDK 17 is at `~/.asdf/installs/java/openjdk-17.0.2`:
  `export JAVA_HOME=~/.asdf/installs/java/openjdk-17.0.2 PATH=$JAVA_HOME/bin:$PATH`
- Verify a branch: `./gradlew :iceberg-core:spotlessApply :iceberg-core:test --tests
  'org.apache.iceberg.TestMapBackedContentStats*' --tests org.apache.iceberg.TestTrackedFileAdapters
  --tests org.apache.iceberg.TestStatsUtilSourceBounds :iceberg-core:checkstyleMain
  :iceberg-core:checkstyleTest`
- Commits carry `Generated-by: OpenCode (Claude Sonnet 5.5)`. If PRs are opened, follow the AI
  disclosure rules in `AGENTS.md` and never self-certify review.

## Working preferences (from the user)

Concise replies; reason in terms of entries; slow down and discuss before building; Ryan Blue is
strict about API surface, so keep new things package-private and prefer extending existing
utilities over adding classes/interfaces.
