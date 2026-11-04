"""D2.1–D2.6: typed models, connectors, transforms, quality and registry."""
import csv
import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from d1.tasks import normalise_ts

class ConfigError(ValueError): pass

@dataclass(frozen=True)
class Order:
    order_id: int
    email: str
    amount: Decimal
    currency: str
    ordered_at: datetime
    def __post_init__(self):
        if self.amount <= 0 or not self.amount.is_finite():
            raise ValueError('Amount must be positive')
        if not re.fullmatch('[A-Z]{3}', self.currency):
            raise ValueError('Currency must be three uppercase letters')
        if self.ordered_at.tzinfo is None or self.ordered_at.utcoffset() is None:
            raise ValueError('Timestamp must be timezone-aware')
        if self.ordered_at.utcoffset() != timedelta(0):
            raise ValueError('Timestamp must be UTC')
    @classmethod
    def from_row(cls, row):
        return cls(int(row['order_id']), row.get('email', row.get('customer_email')), Decimal(str(row['amount'])), row['currency'], datetime.fromisoformat(normalise_ts(row.get('ordered_at', row.get('order_date')), 'UTC')))
    def to_row(self):
        return {'order_id': str(self.order_id), 'email': self.email, 'amount': str(self.amount), 'currency': self.currency, 'ordered_at': self.ordered_at.isoformat()}

class Registered(ABC):
    registry = {}
    def __init_subclass__(cls, type_name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if type_name:
            cls.registry[type_name] = cls

class Connector:
    def open(self): pass
    def close(self): pass
    def __enter__(self):
        self.open()
        return self
    def __exit__(self, *args):
        self.close()

class Source(Connector, Registered):
    registry = {}
    @abstractmethod
    def read(self): ...
    def __or__(self, transform):
        return Pipeline(self, [transform])

class Sink(Connector, Registered):
    registry = {}
    @abstractmethod
    def write(self, records: Iterable[dict]) -> int: ...
    def flush(self): pass

class CsvSource(Source, type_name='csv'):
    def __init__(self, path, **kwargs): self.path = path
    def read(self):
        with open(self.path, newline='', encoding='utf-8') as f:
            yield from csv.DictReader(f)
class JsonLinesSource(Source, type_name='jsonl'):
    def __init__(self, path, **kwargs): self.path = path
    def read(self):
        with open(self.path, encoding='utf-8') as f:
            for line in f:
                if line.strip(): yield json.loads(line)
class InMemorySource(Source, type_name='memory'):
    def __init__(self, records=None, **kwargs): self.records = list(records or [])
    def read(self): yield from self.records

class CsvSink(Sink, type_name='csv'):
    def __init__(self, path, **kwargs): self.path = Path(path)
    def write(self, records):
        iterator = iter(records)
        first = next(iterator, None)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open('w', newline='', encoding='utf-8') as f:
            if first is None: return 0
            writer = csv.DictWriter(f, fieldnames=list(first))
            writer.writeheader()
            writer.writerow(first)
            count = 1
            for row in iterator:
                writer.writerow(row)
                count += 1
        return count
class JsonLinesSink(Sink, type_name='jsonl'):
    def __init__(self, path, **kwargs): self.path = Path(path)
    def write(self, records):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with self.path.open('w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, default=str, ensure_ascii=False) + '\n')
                count += 1
        return count
class InMemorySink(Sink, type_name='memory'):
    def __init__(self, **kwargs): self.records = []
    def write(self, records):
        rows = list(records)
        self.records.extend(rows)
        return len(rows)
class BatchSink(InMemorySink, type_name='batch'):
    """Accepts even small batches; buffers until close."""
    def __init__(self, batch_size=100, **kwargs):
        super().__init__()
        self.batch_size = batch_size
        self.buffer = []
    def write(self, records):
        count = 0
        for row in records:
            self.buffer.append(row)
            count += 1
            if len(self.buffer) >= self.batch_size: self.flush()
        return count
    def flush(self):
        self.records.extend(self.buffer)
        self.buffer.clear()
    def close(self): self.flush()
class DryRunSink(Sink, type_name='dry_run'):
    def write(self, records): return sum(1 for _ in records)
class WarehouseSink(InMemorySink, type_name='warehouse'):
    def flush(self): pass

class Transform(Registered):
    registry = {}
    @abstractmethod
    def apply(self, record): ...
    def __or__(self, other): return TransformChain([self, other])
class TransformChain(Transform):
    def __init__(self, transforms): self.transforms = transforms
    def apply(self, record):
        for transform in self.transforms:
            if record is None: return None
            record = transform.apply(record)
        return record
    def __or__(self, other): return TransformChain(self.transforms + [other])
class Filter(Transform, type_name='filter'):
    def __init__(self, predicate): self.predicate = predicate
    def apply(self, record): return record if self.predicate(record) else None
class Rename(Transform, type_name='rename'):
    def __init__(self, mapping): self.mapping = mapping
    def apply(self, record): return {self.mapping.get(k, k): v for k, v in record.items()}
class Cast(Transform, type_name='cast'):
    def __init__(self, schema):
        self.schema = {'order_id': int, 'amount': Decimal} if schema == 'orders' else schema
        if not isinstance(self.schema, dict): raise ValueError('Invalid schema')
    def apply(self, record):
        result = dict(record)
        for key, converter in self.schema.items():
            if key in result: result[key] = converter(result[key])
        return result
class Deduplicate(Transform, type_name='dedupe'):
    def __init__(self, key): self.key, self.seen = key, set()
    def apply(self, record):
        value = record[self.key]
        if value in self.seen: return None
        self.seen.add(value)
        return record

class Pipeline:
    def __init__(self, source, transforms=None):
        self.source, self.transforms, self.rejects = source, list(transforms or []), []
    def __or__(self, transform): return Pipeline(self.source, self.transforms + [transform])
    def run_records(self):
        with self.source as source:
            for record in source.read():
                try:
                    for transform in self.transforms:
                        record = transform.apply(record)
                        if record is None: break
                    if record is not None: yield record
                except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
                    self.rejects.append({'record': record, 'reason': str(exc)})
    def into(self, sink):
        with sink:
            written = sink.write(self.run_records())
        return {'written': written, 'rejected': len(self.rejects)}

@dataclass
class CheckResult:
    name: str
    passed: bool
    failed_count: int
    sample_failures: list = field(default_factory=list)
    severity: str = 'error'
class Check(Registered):
    registry = {}
    def __init__(self, severity='error'):
        if severity not in ('error', 'warning'): raise ValueError('severity must be error or warning')
        self.severity = severity
    @abstractmethod
    def fails(self, row, seen): ...
    def run(self, records):
        seen, bad, sample = set(), 0, []
        for row in records:
            try: failure = self.fails(row, seen)
            except (ValueError, KeyError, TypeError) as exc: failure = str(exc)
            if failure:
                bad += 1
                if len(sample) < 5: sample.append(str(failure))
        return CheckResult(type(self).__name__, bad == 0, bad, sample, self.severity)
class NotNull(Check, type_name='not_null'):
    def __init__(self, column, severity='error'): super().__init__(severity); self.column = column
    def fails(self, row, seen): return f'{self.column} is null' if row.get(self.column) is None or row.get(self.column) == '' else None
class Unique(Check, type_name='unique'):
    def __init__(self, column, severity='error'): super().__init__(severity); self.column = column
    def fails(self, row, seen):
        value = row[self.column]
        if value in seen: return f'duplicate {self.column}: {value}'
        seen.add(value)
class InRange(Check, type_name='in_range'):
    def __init__(self, column, low, high, severity='error'): super().__init__(severity); self.column, self.low, self.high = column, Decimal(str(low)), Decimal(str(high))
    def fails(self, row, seen): return f'{self.column} outside range' if not self.low <= Decimal(str(row[self.column])) <= self.high else None
class MatchesPattern(Check, type_name='matches_pattern'):
    def __init__(self, column, regex, severity='error'): super().__init__(severity); self.column, self.regex = column, re.compile(regex)
    def fails(self, row, seen): return f'{self.column} does not match' if not self.regex.fullmatch(str(row[self.column])) else None
class Freshness(Check, type_name='freshness'):
    def __init__(self, column, max_age, severity='error', now=None):
        super().__init__(severity)
        self.column, self.max_age, self.now = column, timedelta(seconds=float(max_age)) if not isinstance(max_age, timedelta) else max_age, now
    def fails(self, row, seen):
        now = self.now or datetime.now(timezone.utc)
        dt = datetime.fromisoformat(normalise_ts(row[self.column], 'UTC'))
        return f'{self.column} not fresh' if not timedelta(0) <= now-dt <= self.max_age else None
class QualitySuite:
    def __init__(self, checks): self.checks = checks
    def run(self, records, report_path=None):
        rows = list(records)
        results = [vars(check.run(rows)) for check in self.checks]
        report = {'timestamp': datetime.now(timezone.utc).isoformat(), 'passed': all(r['passed'] or r['severity'] == 'warning' for r in results), 'checks': results}
        if report_path:
            path = Path(report_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        return report

def from_config(spec, registry, location):
    if not isinstance(spec, dict): raise ConfigError(f'{location}: expected object')
    kind = spec.get('type')
    if kind not in registry: raise ConfigError(f'{location}.type: unknown type {kind!r}')
    cls = registry[kind]
    import inspect
    required = [p.name for p in inspect.signature(cls.__init__).parameters.values() if p.name != 'self' and p.default is inspect.Parameter.empty and p.kind not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)]
    for key in required:
        if key not in spec: raise ConfigError(f'{location}.{key}: missing parameter')
    try: return cls(**{k:v for k,v in spec.items() if k != 'type'})
    except (TypeError, ValueError) as exc: raise ConfigError(f'{location}: {exc}') from exc

def pipeline_from_config(config):
    if isinstance(config, (str, Path)):
        with open(config, encoding='utf-8') as f: config = json.load(f)
    source = from_config(config.get('source'), Source.registry, 'source')
    transforms = [from_config(item, Transform.registry, f'transforms[{i}]') for i, item in enumerate(config.get('transforms', []))]
    checks = [from_config(item, Check.registry, f'checks[{i}]') for i, item in enumerate(config.get('checks', []))]
    sink = from_config(config.get('sink'), Sink.registry, 'sink')
    return Pipeline(source, transforms), QualitySuite(checks), sink
