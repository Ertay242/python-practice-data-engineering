# Design note — Mini ETL (one page)

**Architecture.** `Source` and `Sink` are abstract base classes. `CsvSource`, `JsonLinesSource`, and `InMemorySource` inherit from `Source`; their sink counterparts inherit from `Sink`. `Transform` is the common base for `Filter`, `Rename`, `Cast`, and `Deduplicate`. `Check` is the common base for `NotNull`, `Unique`, `InRange`, `MatchesPattern`, and `Freshness`. `Order` is a frozen dataclass and does not inherit from a connector.

**Composition.** A `Pipeline` contains a source and a sequence of transforms; it is not itself a source subclass. `QualitySuite` contains checks. The capstone runner composes a pipeline, quality suite, watermark and partition writer. `|` provides readable transform composition. Registry construction (`__init_subclass__`) avoids type-selection `if` chains.

**Liskov Substitution Principle.** All sinks accept arbitrary iterables (including empty generators) and return a count. The originally broken examples are fixed: `BatchSink` buffers small batches and flushes on close; `DryRunSink` counts consumed records without persistence; `WarehouseSink` supports `flush`. Buffering trades memory for fewer writes. The shared sink contract is checked in tests.

**Reliability.** Invalid CSV rows go to a quarantine file with explicit reasons. Date parsing converts timestamps to UTC before partitioning. Transient failures can be retried with bounded exponential backoff. Incremental watermark state is updated atomically after processing. Quality checks have warning and error severities; error failures mark the run failed. Run reports record counts and duration.

**Known limitations.** Capstone input is materialized in memory to support simple retries; file-level idempotency and record-time watermarks do not provide transactional exactly-once guarantees for all late or updated records. A production implementation needs transactional sink commits, stronger schema enforcement and incremental stream checkpoints.
