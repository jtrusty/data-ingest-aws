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

SCRIPT = Path(__file__).parents[2] / "scripts" / "json_folder_contract_check.py"
spec = importlib.util.spec_from_file_location("json_folder_contract_check", SCRIPT)
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


def test_glue_injected_arguments_do_not_kill_the_parser(monkeypatch, capsys):
    """
    Glue adds --job-name, --scriptLocation, --continuous-log-logGroup and more
    to whatever parameters the job defines. argparse's strict parse_args exits
    2 on those before the script runs, which is how this first failed.
    """
    monkeypatch.setattr(checker.sys, "argv", [
        "check.py", "--uri", "s3://pos/orders", "--days", "3",
        "--job-name", "pos-check", "--JOB_NAME", "pos-check",
        "--scriptLocation", "s3://x/y.py",
        "--continuous-log-logGroup", "/aws-glue/jobs",
        "--job-bookmark-option", "job-bookmark-disable",
    ])

    # Fail at the S3 call, not at argument parsing: getting that far is the
    # assertion. A parse failure would raise SystemExit(2) instead.
    def boom(*_args, **_kwargs):
        raise RuntimeError("reached S3")

    monkeypatch.setattr(checker.boto3, "client", boom)
    with pytest.raises(RuntimeError, match="reached S3"):
        checker.main()
    assert "listing s3://pos/orders" in capsys.readouterr().out


def test_hour_prefixes_render_any_configured_path_format():
    """
    discovery.path_format is configurable, so the checker must validate the
    layout the config actually declares. Prefixes are rendered forward, one
    per hour, rather than parsed back out of keys -- inverting an arbitrary
    strftime pattern is not well defined.
    """
    from datetime import date

    rendered = list(checker.hour_prefixes(
        "orders", "year=%Y/month=%m/day=%d/hour=%H", UTC, date(2026, 9, 10), date(2026, 9, 10)))
    prefixes = [p for p, _ in rendered]
    assert len(prefixes) == 24
    assert prefixes[0] == "orders/year=2026/month=09/day=10/hour=00/"
    assert prefixes[-1] == "orders/year=2026/month=09/day=10/hour=23/"
    # The folder hour is known from the prefix that produced it, not guessed.
    assert rendered[5][1] == (datetime(2026, 9, 10, 5, tzinfo=UTC),) * 2


def test_hour_prefixes_have_no_bucket_prefix_when_the_uri_has_none():
    from datetime import date

    prefixes = [p for p, _ in checker.hour_prefixes(
        "", "%Y/%m/%d/%H", UTC, date(2026, 9, 10), date(2026, 9, 10))]
    assert prefixes[0] == "2026/09/10/00/"


def test_listing_filters_on_the_configured_suffix():
    from unittest.mock import Mock
    from datetime import date

    s3 = Mock()
    s3.get_paginator.return_value.paginate.return_value = [{"Contents": [
        {"Key": "orders/2026/09/10/00/a.jsonl.gz", "LastModified": datetime(2026, 9, 10, 0, 5, tzinfo=UTC)},
        {"Key": "orders/2026/09/10/00/b.json.gz", "LastModified": datetime(2026, 9, 10, 0, 5, tzinfo=UTC)},
    ]}]
    found = list(checker.list_objects(
        s3, "bucket", "orders", date(2026, 9, 10), date(2026, 9, 10), UTC, "%Y/%m/%d/%H", ".jsonl.gz"))
    assert [k for k, _, _ in found] == ["orders/2026/09/10/00/a.jsonl.gz"] * 24


def test_hour_prefixes_step_in_utc_so_a_dst_fall_back_hour_is_not_skipped():
    """
    On 2026-11-01 America/Chicago repeats 01:00 local: 06:00Z and 07:00Z
    both render as .../01/. Stepping a local clock by an hour never lands on
    07:00Z, so objects written then would be reported MISSED although the
    adapter -- which steps in UTC -- would ingest them.
    """
    from datetime import date
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/Chicago")
    rendered = dict(checker.hour_prefixes("pos", "%Y/%m/%d/%H", tz, date(2026, 11, 1), date(2026, 11, 1)))
    # The repeated local hour is one prefix spanning BOTH UTC instants.
    assert rendered["pos/2026/11/01/01/"] == (
        datetime(2026, 11, 1, 6, tzinfo=UTC), datetime(2026, 11, 1, 7, tzinfo=UTC))
    assert rendered["pos/2026/11/01/02/"] == (
        datetime(2026, 11, 1, 8, tzinfo=UTC), datetime(2026, 11, 1, 8, tzinfo=UTC))
    # 25 UTC hours in that local day, 24 distinct prefixes.
    assert len(rendered) == 24

    # And the simulation agrees: objects across the whole repeated hour are found.
    span = rendered["pos/2026/11/01/01/"]
    objects = [(f"k{i}", span,
                datetime(2026, 11, 1, 6, 0, tzinfo=UTC) + timedelta(minutes=10 * i)) for i in range(12)]
    found = checker.simulate(objects, datetime(2026, 11, 1, 5, tzinfo=UTC),
                             timedelta(minutes=15), timedelta(minutes=15), timedelta(seconds=120))
    assert len(found) == 12
