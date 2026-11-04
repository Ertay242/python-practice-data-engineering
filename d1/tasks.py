"""D1.1–D1.6: resilient file and date utilities (standard library only)."""
import csv
import json
import random
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo

FIELDS = ['order_id', 'customer_email', 'phone', 'amount', 'currency', 'order_date']
class ValidationError(Exception): pass
class MissingField(ValidationError): pass
class InvalidAmount(ValidationError): pass
class DuplicateKey(ValidationError): pass
class InvalidDate(ValueError): pass
class RetryExhausted(RuntimeError): pass

def validate_row(row, seen_ids):
    for field in FIELDS:
        if not str(row.get(field) or '').strip():
            raise MissingField(f'Missing field: {field}')
    try:
        amount = Decimal(row['amount'])
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidAmount(f'Invalid amount: {row["amount"]}') from exc
    if not amount.is_finite() or amount < 0:
        raise InvalidAmount('Amount must be non-negative and finite')
    if row['order_id'] in seen_ids:
        raise DuplicateKey(f'Duplicate order_id: {row["order_id"]}')
    seen_ids.add(row['order_id'])
    return row

def ingest(path, clean_path='clean.csv', rejects_path='rejects.csv', fallback_encoding='cp1251'):
    """Decode completely before writing, so a UTF-8 retry never duplicates output."""
    try:
        with open(path, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows, fields = list(reader), reader.fieldnames
    except UnicodeDecodeError:
        with open(path, newline='', encoding=fallback_encoding) as file:
            reader = csv.DictReader(file)
            rows, fields = list(reader), reader.fieldnames
    if not fields or any(field not in fields for field in FIELDS):
        raise ValueError('CSV header must contain: ' + ', '.join(FIELDS))
    seen, clean, rejects, reasons = set(), [], [], Counter()
    for row in rows:
        try:
            validate_row(row, seen)
            clean.append(row)
        except ValidationError as exc:
            reason = type(exc).__name__
            rejects.append({**row, 'reason': f'{reason}: {exc}'})
            reasons[reason] += 1
    for output, columns, data in [(clean_path, fields, clean), (rejects_path, fields + ['reason'], rejects)]:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        with open(output, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
    summary = {'rows_read': len(rows), 'accepted': len(clean), 'rejected': len(rejects), 'reasons': dict(reasons)}
    print(json.dumps(summary, indent=2))
    return summary

def normalise_ts(value, source_tz='UTC', dayfirst=True):
    """Ambiguous slash dates are interpreted using the explicit dayfirst choice."""
    try:
        raw = str(value).strip()
        if raw.endswith('Z'):
            raw = raw[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            fmt = '%d/%m/%Y' if dayfirst else '%m/%d/%Y'
            dt = datetime.strptime(raw, fmt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(source_tz))
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, KeyError) as exc:
        raise InvalidDate(f'Invalid date: {value}') from exc

def partition_path(ts):
    dt = datetime.fromisoformat(normalise_ts(ts, 'UTC'))
    return f'year={dt:%Y}/month={dt:%m}/day={dt:%d}'

def write_partitioned(records, output_dir, date_column='order_date', source_tz='UTC'):
    counts = Counter()
    for record in records:
        ts = normalise_ts(record[date_column], source_tz)
        folder = Path(output_dir) / partition_path(ts)
        folder.mkdir(parents=True, exist_ok=True)
        with (folder / 'data.jsonl').open('a', encoding='utf-8') as file:
            file.write(json.dumps({**record, date_column: ts}, ensure_ascii=False, default=str) + '\n')
        counts[str(folder)] += 1
    return dict(counts)

def normalise_phone(phone):
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits[0] == '8':
        digits = '7' + digits[1:]
    if not re.fullmatch(r'7\d{10}', digits):
        raise ValueError('Invalid phone')
    return re.sub(r'^(\d)(\d{3})(\d{3})(\d{4})$', r'+\1-\2-\3-\4', digits)

def valid_email(email):
    return bool(re.fullmatch(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', str(email)))

def mask_email(email):
    return re.sub(r'([^@\s])[^@\s]*(@[^@\s]+)', lambda m: m.group(1) + '***' + m.group(2), str(email))

def mask_phone(phone):
    return re.sub(r'\d', '*', normalise_phone(phone)[:-4]) + normalise_phone(phone)[-4:]

def strip_html(text):
    return re.sub(r'<[^>]*>', '', str(text))

def incremental_load(folder, watermark_path, process_record):
    """Commit watermark only after a complete file. Bad JSON lines are skipped."""
    watermark_path = Path(watermark_path)
    last = json.loads(watermark_path.read_text(encoding='utf-8'))['date'] if watermark_path.exists() else ''
    counts = {'files': 0, 'records': 0, 'bad_lines': []}
    for file in sorted(Path(folder).glob('*')):
        match = re.fullmatch(r'events_(\d{4}-\d{2}-\d{2})\.jsonl', file.name)
        if not match:
            print(f'Ignored filename: {file.name}')
            continue
        day = match.group(1)
        if day <= last:
            continue
        with file.open(encoding='utf-8') as stream:
            for number, line in enumerate(stream, 1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    counts['bad_lines'].append({'file': file.name, 'line': number})
                    continue
                process_record(record)
                counts['records'] += 1
        watermark_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = watermark_path.with_suffix('.tmp')
        tmp.write_text(json.dumps({'date': day}), encoding='utf-8')
        tmp.replace(watermark_path)
        last = day
        counts['files'] += 1
    return counts

def retry(max_attempts=3, base=0.1, jitter=0.05, sleep=time.sleep, clock=time.monotonic):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start = clock()
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except (ConnectionError, TimeoutError) as exc:
                    if attempt + 1 == max_attempts:
                        raise RetryExhausted(f'{max_attempts} attempts, elapsed {clock()-start:.3f}s') from exc
                    sleep(base * (2 ** attempt) + random.uniform(0, jitter))
        return wrapped
    if max_attempts < 1:
        raise ValueError('max_attempts must be positive')
    return decorate

def sessionise(events, gap_minutes=30):
    by_user = defaultdict(list)
    for event in events:
        by_user[event['user_id']].append((datetime.fromisoformat(normalise_ts(event['timestamp'], 'UTC')), event))
    output = []
    for user, items in by_user.items():
        items.sort(key=lambda pair: pair[0])
        start = end = None
        pages = 0
        def emit():
            output.append({'user_id': user, 'start': start.isoformat(), 'end': end.isoformat(), 'duration_seconds': int((end-start).total_seconds()), 'page_count': pages})
        for stamp, event in items:
            if end is not None and stamp - end > timedelta(minutes=gap_minutes):
                emit()
                start, pages = None, 0
            if start is None:
                start = stamp
            end, pages = stamp, pages + 1
        if start is not None:
            emit()
    return sorted(output, key=lambda row: (str(row['user_id']), row['start']))
