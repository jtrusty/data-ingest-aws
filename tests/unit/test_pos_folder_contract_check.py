"""
The contract checker's verdict is what decides whether prefix-scan discovery
is safe for a producer, so the replay it performs has to match the adapter's
actual rule: prefixes are walked from floor_hour(low) to high, and an object
counts only if its LastModified also falls inside [low, high].
"""

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "pos_folder_contract_check.py"
spec = importlib.util.spec_from_file_location("pos_folder_contract_check", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)

UTC = timezone.utc


def hour(h):
    return datetime(2026, 9, 10, h, tzinfo=UTC)


def run(objects, lookback=15, interval=15, safety=120):
    return checker.simulate(
        objects, hour(8), timedelta(minutes=interval), timedelta(minutes=lookback), timedelta(seconds=safety)
    )


def test_in_hour_and_short_stragglers_are_found():
    objects = [
        ("in-hour", hour(9), hour(9) + timedelta(minutes=40)),
        ("ten-late", hour(9), hour(10) + timedelta(minutes=10)),
    ]
    assert run(objects) == {"in-hour", "ten-late"}


def test_late_arrival_behind_the_prefix_walk_is_missed():
    # Folder 09, written 11:30: by then every run's low bound floors to
    # 10:00 or later, so the 09 prefix is never listed again.
    objects = [("ninety-late", hour(9), hour(11) + timedelta(minutes=30))]
    assert run(objects) == set()
    assert run(objects, lookback=60) == set()
    # Wide enough that low still floors into hour 09 when the object exists.
    assert run(objects, lookback=106) == {"ninety-late"}


def test_next_day_backfill_is_missed():
    objects = [("backfill", hour(9), hour(9) + timedelta(days=1))]
    assert run(objects) == set()


def test_object_written_before_its_folder_hour_is_not_found_by_that_folder():
    # A folder named by event time rather than upload time: object appears
    # in a folder whose hour has not started yet at write time. The walk
    # only lists prefixes up to floor(high), so nothing finds it until the
    # folder hour is reached -- and then LastModified is behind low.
    objects = [("early", hour(12), hour(9))]
    assert run(objects) == set()


def test_safety_delay_defers_but_does_not_lose():
    # Written 30s before a run boundary: excluded by that run's high, caught
    # by the next one via lookback.
    objects = [("edge", hour(9), hour(9) + timedelta(minutes=14, seconds=30))]
    assert run(objects) == {"edge"}


@pytest.mark.parametrize("uri,expected", [
    ("s3://pos-events/orders", ("pos-events", "orders")),
    ("s3://pos-events/orders/", ("pos-events", "orders")),
    ("s3://pos-events", ("pos-events", "")),
])
def test_parse_uri(uri, expected):
    assert checker.parse_uri(uri) == expected


def test_parse_uri_rejects_non_s3():
    with pytest.raises(SystemExit):
        checker.parse_uri("https://pos-events/orders")
