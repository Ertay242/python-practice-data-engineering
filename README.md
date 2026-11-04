# Python Practice — Part 2 (Data Engineering)

Python 3.11+; standard library only for application code. Tests use `pytest`.

## Quick start

Run commands **from the repository root**:

```bash
python -m pip install pytest
python -m pytest -q
python -c "from d1.tasks import ingest; ingest('samples/orders.csv')"
python -m minietl.runner samples/pipeline.json
```

Expected D1.1 sample: 5 input rows, 2 accepted, 3 rejected (invalid amount, missing email, duplicate ID). Output: `clean.csv`, `rejects.csv`.

The ETL example writes `out/run_report.json`, `out/quarantine.jsonl`, `out/clean_orders.jsonl` and UTC partition folders. Run the ETL command twice to see the watermark skip already processed rows.

## Task map

- D1.1 `d1.tasks.ingest`, `validate_row`: validation, quarantine and fallback encoding.
- D1.2 `normalise_ts`, `partition_path`, `write_partitioned`: UTC and partitioning.
- D1.3 `normalise_phone`, `valid_email`, `mask_email`, `mask_phone`, `strip_html`.
- D1.4 `incremental_load`: filename-based watermark, atomic state updates, JSONL recovery.
- D1.5 `retry`: retry transient exceptions only; injectable sleep and clock.
- D1.6 `sessionise`: sorted UTC sessions with 30-minute idle gap.
- D2.1 `Order`: immutable, validated model and round-trip conversion.
- D2.2 Sources and sinks: ABCs, context managers, CSV/JSONL/memory.
- D2.3 Transform hierarchy and `Pipeline`, `|` composition and rejects.
- D2.4 Checks and `QualitySuite`: reports and warning/error severities.
- D2.5 `BatchSink`, `DryRunSink`, `WarehouseSink`: corrected LSP contracts.
- D2.6 `pipeline_from_config`: registry populated via `__init_subclass__`.
- Capstone `minietl.runner`: combines the parts, writes partitions and run report.

## Design decisions and limitations

- `01/10/2026` is ambiguous: default `dayfirst=True` means **1 October**, not January 10. Offset-aware timestamps keep their offset before UTC conversion.
- D1.1 deliberately does **not** reject invalid email formats or dates; those belong to D1.2 and D1.3.
- Regex HTML tag stripping is acceptable only for a small controlled product-description field; real-world malformed HTML needs an HTML parser.
- LSP: a sink must accept **any iterable**, including an empty one, and return an integer count. `BatchSink` buffers short writes instead of rejecting them (memory trade-off); `DryRunSink` returns the number consumed; `WarehouseSink.flush` is supported.
- D1.4 file-level watermark assumes append-only daily files; for true exactly-once side effects, the sink and watermark need a transaction or deduplication keys.
- Capstone is a **small educational framework**. It materializes input for retries and checks, so it is not suitable for large production datasets. Watermark is based on record timestamps; late-arriving older timestamps need a configurable lookback window. A corrupt entire source file fails the run and is reported, whereas malformed individual records are quarantined.
- Source/sink context managers close resources inside iterator/write methods. Production implementations would additionally manage persistent connections.
