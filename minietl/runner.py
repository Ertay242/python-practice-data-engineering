"""Capstone CLI: python -m minietl.runner samples/pipeline.json"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from d1.tasks import normalise_ts, partition_path, retry
from d2.framework import pipeline_from_config

def utcnow(): return datetime.now(timezone.utc).isoformat()

def run(config_path):
    started, clock = utcnow(), time.monotonic()
    path = Path(config_path)
    config = json.loads(path.read_text(encoding='utf-8'))
    # Relative paths in config are resolved from the repository working directory.
    output = Path(config.get('output_dir', 'out/partitioned'))
    report_path = Path(config.get('report_path', 'out/run_report.json'))
    watermark_path = Path(config.get('watermark_path', 'out/watermark.json'))
    quarantine_path = Path(config.get('quarantine_path', 'out/quarantine.jsonl'))
    pipeline, quality, sink = pipeline_from_config(config)
    old = json.loads(watermark_path.read_text(encoding='utf-8')).get('watermark', '') if watermark_path.exists() else ''
    date_key = config.get('date_column', 'order_date')
    source_tz = config.get('source_tz', 'UTC')
    rows_in, rejects, accepted, skipped = 0, [], [], 0
    @retry(max_attempts=config.get('retry_attempts', 3), base=0.01)
    def load_source():
        # Source.read() is lazy; materialization here is deliberate for retry and quality checks.
        with pipeline.source as source:
            return list(source.read())
    try:
        raw = load_source()
        for row in raw:
            rows_in += 1
            try:
                ts = normalise_ts(row[date_key], source_tz)
                if ts <= old:
                    skipped += 1
                    continue
                current = dict(row)
                for transform in pipeline.transforms:
                    current = transform.apply(current)
                    if current is None: break
                if current is None:
                    skipped += 1
                    continue
                accepted.append((ts, current))
            except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                rejects.append({'record': row, 'reason': str(exc)})
        quality_report = quality.run([r for _, r in accepted])
        # Partition by UTC, and write via the configured sink to a staging file first.
        if accepted:
            by_partition = {}
            for ts, record in accepted:
                by_partition.setdefault(partition_path(ts), []).append(record)
            for partition, records in by_partition.items():
                target = output / partition / 'data.jsonl'
                target.parent.mkdir(parents=True, exist_ok=True)
                # Replace a partition, rather than append, to avoid duplicates on reruns.
                existing = []
                if target.exists():
                    existing = [json.loads(line) for line in target.read_text(encoding='utf-8').splitlines() if line]
                seen = {json.dumps(row, sort_keys=True, default=str) for row in existing}
                with target.open('a', encoding='utf-8') as stream:
                    for record in records:
                        signature = json.dumps(record, sort_keys=True, default=str)
                        if signature not in seen:
                            stream.write(json.dumps(record, default=str, ensure_ascii=False) + '\n')
                            seen.add(signature)
            with sink:
                sink.write((r for _, r in accepted))
        if rejects:
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            with quarantine_path.open('a', encoding='utf-8') as stream:
                for row in rejects: stream.write(json.dumps(row, default=str, ensure_ascii=False) + '\n')
        status = 'success' if quality_report['passed'] else 'failed_quality'
        if status == 'success' and accepted:
            watermark_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = watermark_path.with_suffix('.tmp')
            tmp.write_text(json.dumps({'watermark': max(ts for ts, _ in accepted)}), encoding='utf-8')
            tmp.replace(watermark_path)
    except Exception as exc:
        status = 'failed'
        quality_report = {'passed': False, 'checks': [], 'error': str(exc)}
    report = {'started_at': started, 'ended_at': utcnow(), 'duration_seconds': round(time.monotonic()-clock, 3), 'status': status, 'rows_in': rows_in, 'rows_out': len(accepted), 'rows_rejected': len(rejects), 'rows_skipped': skipped, 'rejected_by_step': {'validation_or_transform': len(rejects)}, 'quality': quality_report}
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    print(json.dumps(report, indent=2))
    return report

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python -m minietl.runner samples/pipeline.json')
    result = run(sys.argv[1])
    sys.exit(0 if result['status'] == 'success' else 1)
