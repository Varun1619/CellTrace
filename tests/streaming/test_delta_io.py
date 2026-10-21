import pytest

pytest.importorskip("pyspark")

from streaming.delta_io import with_conflict_retry  # noqa: E402


class ConcurrentAppendException(Exception):
    pass


def test_retries_concurrency_conflicts_then_succeeds():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConcurrentAppendException("Files were added to the root of the table by a concurrent update")
        return "ok"

    assert with_conflict_retry(flaky, base_delay_s=0) == "ok"
    assert len(calls) == 3


def test_other_errors_are_not_retried():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError("schema mismatch")

    with pytest.raises(ValueError):
        with_conflict_retry(broken, base_delay_s=0)
    assert len(calls) == 1


def test_gives_up_after_the_last_attempt():
    def always():
        raise ConcurrentAppendException("still conflicting")

    with pytest.raises(ConcurrentAppendException):
        with_conflict_retry(always, attempts=2, base_delay_s=0)
