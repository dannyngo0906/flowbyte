"""Unit tests for HaravanTokenBucket — pure logic, no IO."""
from __future__ import annotations

import threading
import time

import pytest
from freezegun import freeze_time

from flowbyte.haravan.token_bucket import HaravanTokenBucket


@pytest.fixture
def bucket():
    return HaravanTokenBucket()


def test_initial_state_empty(bucket):
    assert bucket._tokens_used == 0.0


def test_acquire_increments_tokens(bucket):
    bucket.acquire()
    assert bucket._tokens_used == 1.0


def test_acquire_multiple_increments(bucket):
    for _ in range(5):
        bucket.acquire()
    assert abs(bucket._tokens_used - 5.0) < 0.01


def test_acquire_blocks_at_safety_margin():
    bucket = HaravanTokenBucket()
    bucket._tokens_used = float(HaravanTokenBucket.SAFETY_MARGIN) + 1.0
    bucket._last_leak = time.monotonic()

    acquired = threading.Event()

    def try_acquire():
        bucket.acquire()
        acquired.set()

    t = threading.Thread(target=try_acquire, daemon=True)
    t.start()
    t.join(timeout=0.5)
    assert not acquired.is_set(), "acquire() should block when tokens_used >= SAFETY_MARGIN"
    # Clean up — let the bucket drain and thread finish
    bucket._tokens_used = 0.0
    t.join(timeout=2.0)


def test_update_from_header_sets_token_count(bucket):
    bucket.update_from_header("45/80")
    assert bucket._tokens_used == 45.0


def test_update_from_header_zero(bucket):
    bucket._tokens_used = 60.0
    bucket.update_from_header("0/80")
    assert bucket._tokens_used == 0.0


def test_update_from_header_full(bucket):
    bucket.update_from_header("80/80")
    assert bucket._tokens_used == 80.0


def test_update_from_header_malformed_does_not_crash(bucket, caplog):
    import structlog
    initial = bucket._tokens_used
    bucket.update_from_header("bad/data/format")
    assert bucket._tokens_used == initial  # unchanged


def test_update_from_header_empty_string_does_not_crash(bucket):
    initial = bucket._tokens_used
    bucket.update_from_header("")
    assert bucket._tokens_used == initial


def test_update_from_header_with_spaces(bucket):
    bucket.update_from_header("  30  /  80  ")
    assert bucket._tokens_used == 30.0


def test_leak_reduces_tokens_over_time():
    bucket = HaravanTokenBucket()
    bucket._tokens_used = 20.0
    bucket._last_leak = time.monotonic() - 1.0  # 1 second ago

    with bucket._lock:
        bucket._leak()

    # Should have leaked ~4 tokens in 1 second (LEAK_RATE = 4.0)
    assert bucket._tokens_used < 20.0
    assert bucket._tokens_used >= 0.0


def test_leak_never_goes_below_zero():
    bucket = HaravanTokenBucket()
    bucket._tokens_used = 2.0
    bucket._last_leak = time.monotonic() - 10.0  # 10 seconds ago → 40 tokens leaked

    with bucket._lock:
        bucket._leak()

    assert bucket._tokens_used == 0.0


def test_acquire_after_update_from_header_respects_safety_margin():
    bucket = HaravanTokenBucket()
    # Server says we used 69 tokens → 1 below SAFETY_MARGIN (70)
    bucket.update_from_header("69/80")

    # Should be able to acquire once more (69 → 70, hits margin)
    acquired = threading.Event()

    def do_acquire():
        bucket.acquire()
        acquired.set()

    t = threading.Thread(target=do_acquire, daemon=True)
    t.start()
    t.join(timeout=1.0)
    assert acquired.is_set()


def test_thread_safety_concurrent_acquire():
    """Multiple threads acquiring concurrently should not corrupt state."""
    bucket = HaravanTokenBucket()
    results = []
    errors = []

    def worker():
        try:
            bucket.acquire()
            results.append(1)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)

    assert not errors
    assert len(results) == 10
    assert abs(bucket._tokens_used - 10.0) < 0.1


def test_constants():
    assert HaravanTokenBucket.CAPACITY == 80
    assert HaravanTokenBucket.LEAK_RATE == 4.0
    assert HaravanTokenBucket.SAFETY_MARGIN == 70
