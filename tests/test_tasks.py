import csv
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pytest
from d1.tasks import *
from d2.framework import *

def test_ingest(tmp_path):
    from pathlib import Path
    summary = ingest('samples/orders.csv', tmp_path/'clean.csv', tmp_path/'rejects.csv')
    assert (summary['rows_read'], summary['accepted'], summary['rejected']) == (5,2,3)
    assert set(summary['reasons']) == {'InvalidAmount','MissingField','DuplicateKey'}

def test_dates():
    assert partition_path(normalise_ts('2026-11-01T01:30:00', 'Asia/Tokyo')) == 'year=2026/month=10/day=31'
    assert partition_path(normalise_ts('2026-10-31T23:30:00', 'Asia/Tokyo')) == 'year=2026/month=10/day=31'
    with pytest.raises(InvalidDate): normalise_ts('2026-13-01', 'UTC')

def test_pii():
    assert normalise_phone('8(701)1234568') == '+7-701-123-4568'
    assert mask_email('aru@example.com') == 'a***@example.com'
    assert valid_email('aru@example.com') and not valid_email('dana@example')

def test_retry():
    calls = []
    @retry(max_attempts=3, base=0, jitter=0, sleep=lambda x: None)
    def flaky():
        calls.append(1)
        if len(calls) < 3: raise ConnectionError('offline')
        return 7
    assert flaky() == 7
    @retry(max_attempts=2, base=0, jitter=0, sleep=lambda x: None)
    def broken(): raise TimeoutError()
    with pytest.raises(RetryExhausted): broken()

def test_sessions():
    rows = [{'user_id':'a','timestamp':'2026-10-01T00:40:00Z','url':'/'}, {'user_id':'a','timestamp':'2026-10-01T00:00:00Z','url':'/'}, {'user_id':'a','timestamp':'2026-10-01T00:20:00Z','url':'/'}]
    assert [r['page_count'] for r in sessionise(rows)] == [3]

def test_order_roundtrip():
    o = Order(1, 'a@example.com', Decimal('10.50'), 'USD', datetime.now(timezone.utc))
    assert Order.from_row(o.to_row()) == o
    with pytest.raises(ValueError): Order(1, 'x', Decimal('-1'), 'USD', datetime.now(timezone.utc))

@pytest.mark.parametrize('source_cls', [CsvSource, JsonLinesSource, InMemorySource])
@pytest.mark.parametrize('sink_cls', [CsvSink, JsonLinesSink, InMemorySink])
def test_source_sink_contract(tmp_path, source_cls, sink_cls):
    rows = [{'id':'1','value':'x'}]
    input_file = tmp_path / ('input.csv' if source_cls is CsvSource else 'input.jsonl')
    if source_cls is CsvSource:
        with input_file.open('w', newline='', encoding='utf-8') as f:
            writer=csv.DictWriter(f, fieldnames=['id','value']);writer.writeheader();writer.writerows(rows)
    elif source_cls is JsonLinesSource: input_file.write_text(json.dumps(rows[0])+'\n',encoding='utf-8')
    source = source_cls(rows) if source_cls is InMemorySource else source_cls(input_file)
    output_file = tmp_path / ('output.csv' if sink_cls is CsvSink else 'output.jsonl')
    sink = sink_cls() if sink_cls is InMemorySink else sink_cls(output_file)
    with source, sink:
        assert sink.write(source.read()) == 1
        assert sink.write([]) == 0

def test_pipeline():
    p = InMemorySource([{'id':'1'}, {'id':'1'}, {'id':'2'}]) | Deduplicate('id') | Rename({'id':'key'})
    sink = InMemorySink()
    assert p.into(sink)['written'] == 2

def test_quality():
    suite = QualitySuite([NotNull('a'), Unique('a')])
    report = suite.run([{'a':'x'},{'a':'x'},{'a':None}])
    assert not report['passed']
    assert report['checks'][0]['failed_count'] == 1

def test_registry_errors():
    with pytest.raises(ConfigError, match=r'transforms\[1\]\.key'):
        from_config({'type':'dedupe'}, Transform.registry, 'transforms[1]')

def test_incremental(tmp_path):
    folder=tmp_path/'events';folder.mkdir()
    (folder/'events_2026-10-01.jsonl').write_text('{"x":1}\nBAD\n',encoding='utf-8')
    output=[]
    assert incremental_load(folder,tmp_path/'state.json',output.append)['records']==1
    assert incremental_load(folder,tmp_path/'state.json',output.append)['records']==0
    (folder/'events_2026-10-02.jsonl').write_text('{"x":2}\n',encoding='utf-8')
    assert incremental_load(folder,tmp_path/'state.json',output.append)['records']==1

def test_sink_lsp():
    for sink in (InMemorySink(), BatchSink(), DryRunSink(), WarehouseSink()):
        with sink:
            assert sink.write([]) == 0
            assert sink.write(iter([{'x':1}])) == 1
            sink.flush()
