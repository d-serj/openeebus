"""Observation regressions; no network or demo processes are required."""
from pathlib import Path

import pytest

from _records import records_since, matches, timestamp_seconds
from conftest import NodeProcess
from test_close_timing import _close_duration_ms


def test_fragmented_record(tmp_path):
    path = tmp_path / 'stdout'
    path.write_bytes(b'EG LPC value=12')
    assert records_since(path) == []
    with path.open('ab') as stream:
        stream.write(b'34 from entity: hp/1\n')
    assert records_since(path) == ['EG LPC value=1234 from entity: hp/1\n']


def test_partial_old_record_is_not_new(tmp_path):
    path = tmp_path / 'stdout'
    path.write_bytes(b'old notification')
    position = path.stat().st_size
    with path.open('ab') as stream:
        stream.write(b' value=1\nnew value=2\n')
    assert records_since(path, position) == ['new value=2\n']


def test_stale_wrong_peer_and_wrong_use_case(tmp_path):
    path = tmp_path / 'stdout'
    path.write_text('EG LPC value=1 from entity: hp/1\n')
    position = path.stat().st_size
    with path.open('a') as stream:
        stream.write('EG LPC value=2 from entity: ev/1\nEG LPP value=2 from entity: hp/1\n')
    assert not any(matches(line, 'EG LPC', 'hp/1', 'value=2') for line in records_since(path, position))


@pytest.mark.parametrize('text,expected', [
    ('[2026/10/25 01:02:03:1234] test', 3723.1234),
    ('01:02:03.1234 test', 3723.1234),
    ('01:02:03.000001 test', 3723.000001),
    ('[01:02:03.999999] test', 3723.999999),
    ('25:02:03.1 test', None), ('01:61:03.1 test', None), ('missing', None),
])
def test_timestamp_formats(text, expected):
    actual = timestamp_seconds(text)
    if expected is None:
        assert actual is None
    else:
        assert actual % 86400 == pytest.approx(expected, abs=1e-5)


def test_close_midnight_and_missing_record(tmp_path):
    path = tmp_path / 'stdout'
    path.write_text('23:59:59.9000 EebusService::Stop(): begin\n'
                    '00:00:00.1000 EebusService::Destruct(): end\n')
    assert _close_duration_ms(path) == 200
    path.write_text('23:59:59.9000 EebusService::Stop(): begin\n'
                    '00:00:00.1000 EebusService::Destruct(): end')
    assert _close_duration_ms(path) is None


def test_process_exit_cannot_satisfy_missing_record(tmp_path):
    class Exited:
        def poll(self): return 9
    node = object.__new__(NodeProcess)
    node.log_path = str(tmp_path / 'stdout')
    Path(node.log_path).write_text('incomplete')
    node.proc = Exited()
    with pytest.raises(AssertionError, match='exited 9'):
        node.wait_for_new_line('missing', 0, 0.1)


def test_record_guard_excludes_other_threads(tmp_path):
    import subprocess
    root = Path(__file__).resolve().parents[2]
    source = tmp_path / 'record.c'
    source.write_text(r'''
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include "src/common/output_record.h"
static atomic_bool checked;
static int acquired;
static void* Probe(void* ctx) {
  (void)ctx;
  acquired = ftrylockfile(stdout) == 0;
  if (acquired) funlockfile(stdout);
  atomic_store(&checked, 1);
  return NULL;
}
int main(void) {
  pthread_t worker;
#ifndef CONTROL
  OutputRecordBegin();
#endif
  if (pthread_create(&worker, NULL, Probe, NULL)) return 2;
  while (!atomic_load(&checked)) {}
#ifndef CONTROL
  OutputRecordEnd();
#endif
  pthread_join(worker, NULL);
  return acquired ? 1 : 0;
}
''')
    binary = tmp_path / 'record'
    for flags, expected in [([], 0), (['-DCONTROL'], 1)]:
        subprocess.run(['/usr/bin/gcc-13', '-std=c11', '-D_POSIX_C_SOURCE=200809L',
                        '-Wall', '-Wextra', '-Werror', '-pthread', '-I', str(root),
                        *flags, str(source), '-o', str(binary)], check=True)
        assert subprocess.run([str(binary)], timeout=5).returncode == expected


def test_real_entity_record_and_missing_identity():
    from _records import record_entity
    line = 'EG LPC received 5000W from entity: {"": [{"device": "hp"}, {"entity": [1, 2]}]}\n'
    assert record_entity(line) == 'hp/1/2'
    assert matches(line, 'EG LPC', 'hp/1/2', '5000W')
    assert not matches(line, 'EG LPC', 'hp/1/2', '500W')
    assert record_entity('EG LPC received 5000W from entity: {broken}\n') is None


def test_invalid_date_is_not_a_timestamp():
    assert timestamp_seconds('[2026/99/25 01:02:03:1234]') is None


def test_peer_identity_does_not_accept_another_ready_peer():
    from _records import matches
    record = 'MA MPC remote entity connected ski=aaaa from entity: hp/1\n'
    assert matches(record, 'MA MPC remote entity connected', entity='hp/1', peer_ski='aaaa')
    assert not matches(record, 'MA MPC remote entity connected', entity='hp/1', peer_ski='bbbb')
    assert not matches(record, 'MA MPC remote entity connected', peer_ski='aaa')
