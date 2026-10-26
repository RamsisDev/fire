"""Shard following: where each shard starts by start position and listing, splits and merges,
expired and trimmed iterators, and failed calls logged once, on a stub stream.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aws_streams import Passes, drain, nothing
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from fire.adapters.sources.aws import FailureLog, ReadProblem
from fire.adapters.sources.shards import (
    AFTER_SEQUENCE_NUMBER,
    AT_TIMESTAMP,
    CLOCK_SKEW_S,
    LATEST,
    TRIM_HORIZON,
    Page,
    Shard,
    follow,
)
from fire.core.ports.record_source import StartPosition

LOGGER = logging.getLogger("tests.shards")
T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
SKEW = timedelta(seconds=CLOCK_SKEW_S)
WARNING = "event read failed: source=events region=us-east-1 error="


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "planted text"}}, "GetRecords")


@dataclass
class _Shard:
    records: list[str] = field(default_factory=list)
    closed: bool = False


@dataclass
class StubStream:
    """Shards of numbered records; an iterator is `<shard>@<index>` and a page holds one record.

    `errors` maps a call name to the errors its next calls raise. `written` holds when a record
    was written, for AT_TIMESTAMP, which only a stream with `timestamps` takes. A sequence
    number the shard does not hold is an `InvalidArgumentException`.
    """

    by_id: dict[str, _Shard]
    stream: str = "stream-1"
    timestamps: bool = True
    errors: dict[str, list[Exception]] = field(default_factory=dict)
    iterators: list[tuple[str, str, Any]] = field(default_factory=list)
    written: dict[str, datetime] = field(default_factory=dict)

    def _raise(self, call: str) -> None:
        if self.errors.get(call):
            raise self.errors[call].pop(0)

    def locate(self) -> str:
        self._raise("locate")
        return self.stream

    def shards(self, stream: str) -> list[Shard]:
        self._raise("shards")
        return [Shard(shard_id, shard.closed) for shard_id, shard in self.by_id.items()]

    def iterator(
        self, stream: str, shard_id: str, kind: str, after: str | None, at: datetime | None
    ) -> str:
        self._raise("iterator")
        self.iterators.append((shard_id, kind, after if at is None else at))
        records = self.by_id[shard_id].records
        if kind == TRIM_HORIZON:
            index = 0
        elif kind == LATEST:
            index = len(records)
        elif kind == AT_TIMESTAMP:
            assert self.timestamps and at is not None
            later = [i for i, record in enumerate(records) if self.written.get(record, T0) >= at]
            index = later[0] if later else len(records)
        else:
            assert kind == AFTER_SEQUENCE_NUMBER
            if after not in records:
                raise _client_error("InvalidArgumentException")
            index = records.index(after) + 1
        return f"{shard_id}@{index}"

    def page(self, iterator: str) -> Page:
        self._raise("page")
        shard_id, _, index = iterator.partition("@")
        shard, at = self.by_id[shard_id], int(index)
        if at < len(shard.records):
            return Page([shard.records[at]], f"{shard_id}@{at + 1}")
        return Page([], None if shard.closed else iterator)

    def sequence(self, record: Any) -> str:
        return record


def _follow(
    stub: StubStream,
    *steps: Callable[[], object],
    start: StartPosition = StartPosition.OLDEST,
    clock: Callable[[], float] = lambda: 0.0,
    now: Callable[[], datetime] = lambda: T0,
    noted: list[str] | None = None,
) -> Any:
    return follow(
        stub,
        start=start,
        poll_s=1,
        failures=FailureLog(LOGGER, "event read failed", "events", "us-east-1"),
        sleep=Passes(*steps),
        clock=clock,
        now=now,
        note=(noted if noted is not None else []).append,
    )


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == LOGGER.name]


@pytest.mark.unit
async def test_oldest_reads_every_shard_from_its_first_record_one_page_per_pass() -> None:
    stub = StubStream({"s-0": _Shard(["a1", "a2"]), "s-1": _Shard(["b1"], closed=True)})

    read = await drain(_follow(stub, nothing, nothing))

    assert read == ["a1", "b1", "a2"]
    assert stub.iterators == [("s-0", TRIM_HORIZON, None), ("s-1", TRIM_HORIZON, None)]


@pytest.mark.unit
async def test_latest_starts_open_shards_at_their_tip_and_skips_closed_ones() -> None:
    stub = StubStream({"s-0": _Shard(["old"]), "s-1": _Shard(["gone"], closed=True)})

    read = await drain(
        _follow(stub, lambda: stub.by_id["s-0"].records.append("new"), start=StartPosition.LATEST)
    )

    assert read == ["new"]
    assert stub.iterators == [("s-0", LATEST, None)]


@pytest.mark.unit
async def test_children_of_a_split_are_read_from_their_first_record() -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})

    def split() -> None:
        stub.by_id["s-0"].closed = True
        stub.by_id["s-1"] = _Shard(["b1", "b2"])
        stub.by_id["s-2"] = _Shard(["c1"])

    read = await drain(_follow(stub, split, nothing, nothing, start=StartPosition.LATEST))

    assert read == ["b1", "c1", "b2"]
    assert stub.iterators == [
        ("s-0", LATEST, None),
        ("s-1", TRIM_HORIZON, None),
        ("s-2", TRIM_HORIZON, None),
    ]


@pytest.mark.unit
async def test_shards_are_listed_again_every_refresh_interval() -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})
    now = [0.0]

    def new_shard_after_a_minute() -> None:
        stub.by_id["s-1"] = _Shard(["b1"])
        now[0] = 61.0

    read = await drain(_follow(stub, nothing, new_shard_after_a_minute, clock=lambda: now[0]))

    assert read == ["a1", "b1"]


@pytest.mark.unit
async def test_an_expired_iterator_resumes_after_the_last_record_read(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard(["a1", "a2", "a3"])})

    def expire() -> None:
        stub.errors["page"] = [_client_error("ExpiredIteratorException")]

    read = await drain(_follow(stub, expire, nothing, nothing))

    assert read == ["a1", "a2", "a3"]
    assert stub.iterators == [("s-0", TRIM_HORIZON, None), ("s-0", AFTER_SEQUENCE_NUMBER, "a1")]
    assert _warnings(caplog) == []


@pytest.mark.unit
async def test_trimmed_data_restarts_the_shard_at_its_oldest_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard(["a1", "a2"])})

    def trim() -> None:
        stub.errors["page"] = [_client_error("TrimmedDataAccessException")]

    read = await drain(_follow(stub, trim, nothing, nothing))

    assert read == ["a1", "a1", "a2"]
    assert stub.iterators[-1] == ("s-0", TRIM_HORIZON, None)
    assert _warnings(caplog) == [f"{WARNING}ClientError:TrimmedDataAccessException"]


@pytest.mark.unit
async def test_failures_are_logged_once_until_a_pass_has_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})
    stub.errors["locate"] = [
        _client_error("ResourceNotFoundException"),
        _client_error("ResourceNotFoundException"),
        ReadProblem("no_stream"),
    ]
    stub.errors["page"] = [EndpointConnectionError(endpoint_url="http://planted.example")]

    read = await drain(_follow(stub, nothing, nothing, nothing, nothing, nothing))

    assert read == ["a1"]
    assert _warnings(caplog) == [
        f"{WARNING}ClientError:ResourceNotFoundException",
        f"{WARNING}no_stream",
        f"{WARNING}EndpointConnectionError",
    ]
    assert "planted" not in caplog.text


@pytest.mark.unit
async def test_a_failed_page_leaves_the_other_shards_of_its_pass() -> None:
    stub = StubStream({"s-0": _Shard(["a1"]), "s-1": _Shard(["b1"])})
    stub.errors["page"] = [_client_error("ProvisionedThroughputExceededException")]

    read = await drain(_follow(stub, nothing))

    assert read == ["b1", "a1"]


@pytest.mark.unit
async def test_a_new_stream_starts_over_from_the_oldest_record_of_its_shards() -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})
    now = [0.0]

    def replace_the_stream() -> None:
        stub.stream = "stream-2"
        stub.by_id = {"s-0": _Shard(["n1"])}
        now[0] = 61.0

    read = await drain(
        _follow(
            stub, replace_the_stream, nothing, start=StartPosition.LATEST, clock=lambda: now[0]
        )
    )

    assert read == ["n1"]
    assert stub.iterators == [("s-0", LATEST, None), ("s-0", TRIM_HORIZON, None)]


@pytest.mark.unit
async def test_a_programming_error_propagates() -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})
    stub.errors["page"] = [KeyError("Records")]

    with pytest.raises(KeyError):
        await drain(_follow(stub, nothing))


def _written_after_the_first_pass(stub: StubStream, record: str, error: str) -> Callable[[], None]:
    """A step: `record` lands in the shard a second after T0, and the next page fails."""

    def step() -> None:
        stub.by_id["s-0"].records.append(record)
        stub.written[record] = T0 + timedelta(seconds=1)
        stub.errors["page"] = [_client_error(error)]

    return step


@pytest.mark.unit
@pytest.mark.parametrize(
    "error",
    ["ProvisionedThroughputExceededException", "InternalFailure", "LimitExceededException"],
)
async def test_throttled_at_latest_before_the_first_record_keeps_its_iterator(
    caplog: pytest.LogCaptureFixture, error: str
) -> None:
    stub = StubStream({"s-0": _Shard()})
    write = _written_after_the_first_pass(stub, "new", error)

    read = await drain(_follow(stub, write, nothing, nothing, start=StartPosition.LATEST))

    assert read == ["new"]
    assert stub.iterators == [("s-0", LATEST, None)]
    assert _warnings(caplog) == [f"{WARNING}ClientError:{error}"]


@pytest.mark.unit
async def test_a_timeout_keeps_the_iterator_too() -> None:
    stub = StubStream({"s-0": _Shard()})

    def write() -> None:
        stub.by_id["s-0"].records.append("new")
        stub.errors["page"] = [ReadTimeoutError(endpoint_url="http://planted.example")]

    read = await drain(_follow(stub, write, nothing, nothing, start=StartPosition.LATEST))

    assert (read, stub.iterators) == (["new"], [("s-0", LATEST, None)])


@pytest.mark.unit
async def test_expired_at_latest_before_the_first_record_resumes_at_the_last_page_time(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard()})
    clock = [T0]
    write = _written_after_the_first_pass(stub, "new", "ExpiredIteratorException")

    def write_then_wait() -> None:
        write()
        clock[0] = T0 + timedelta(minutes=6)

    read = await drain(
        _follow(stub, write_then_wait, nothing, start=StartPosition.LATEST, now=lambda: clock[0])
    )

    assert read == ["new"]
    assert stub.iterators == [("s-0", LATEST, None), ("s-0", AT_TIMESTAMP, T0 - SKEW)]
    assert _warnings(caplog) == []


@pytest.mark.unit
async def test_a_gone_stream_at_latest_resumes_at_the_last_page_time() -> None:
    stub = StubStream({"s-0": _Shard()})
    write = _written_after_the_first_pass(stub, "new", "ResourceNotFoundException")

    read = await drain(_follow(stub, write, nothing, nothing, start=StartPosition.LATEST))

    assert read == ["new"]
    assert stub.iterators == [("s-0", LATEST, None), ("s-0", AT_TIMESTAMP, T0 - SKEW)]


@pytest.mark.unit
async def test_a_client_clock_ahead_by_less_than_the_margin_still_yields_the_record() -> None:
    stub = StubStream({"s-0": _Shard()})
    write = _written_after_the_first_pass(stub, "new", "ExpiredIteratorException")
    ahead = T0 + timedelta(seconds=5)

    read = await drain(
        _follow(stub, write, nothing, start=StartPosition.LATEST, now=lambda: ahead)
    )

    assert read == ["new"]
    assert stub.iterators[-1] == ("s-0", AT_TIMESTAMP, ahead - SKEW)


@pytest.mark.unit
async def test_a_resume_never_asks_for_a_time_later_than_now_minus_the_margin() -> None:
    stub = StubStream({"s-0": _Shard()})
    clock = [T0 + timedelta(hours=1)]
    write = _written_after_the_first_pass(stub, "new", "ExpiredIteratorException")

    def write_with_the_clock_set_back() -> None:
        write()
        clock[0] = T0

    read = await drain(
        _follow(
            stub,
            write_with_the_clock_set_back,
            nothing,
            start=StartPosition.LATEST,
            now=lambda: clock[0],
        )
    )

    assert read == ["new"]
    assert stub.iterators[-1] == ("s-0", AT_TIMESTAMP, T0 - SKEW)


@pytest.mark.unit
async def test_a_refused_timestamp_starts_the_shard_at_its_tip_and_notes_the_position_lost(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard()})
    noted: list[str] = []

    def expire_and_refuse_the_resume() -> None:
        stub.by_id["s-0"].records.append("lost")
        stub.errors["page"] = [_client_error("ExpiredIteratorException")]
        stub.errors["iterator"] = [_client_error("InvalidArgumentException")]

    read = await drain(
        _follow(
            stub,
            expire_and_refuse_the_resume,
            nothing,
            lambda: stub.by_id["s-0"].records.append("later"),
            nothing,
            start=StartPosition.LATEST,
            noted=noted,
        )
    )

    assert read == ["later"]
    assert stub.iterators == [("s-0", LATEST, None), ("s-0", LATEST, None)]
    assert noted == ["position_lost"]
    assert _warnings(caplog) == [
        f"{WARNING}ClientError:InvalidArgumentException",
        f"{WARNING}position_lost",
    ]


@pytest.mark.unit
async def test_without_timestamps_an_expired_iterator_at_latest_logs_the_position_lost(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard()}, timestamps=False)
    write = _written_after_the_first_pass(stub, "lost", "ExpiredIteratorException")
    noted: list[str] = []

    read = await drain(
        _follow(
            stub,
            write,
            lambda: stub.by_id["s-0"].records.append("later"),
            nothing,
            start=StartPosition.LATEST,
            noted=noted,
        )
    )

    assert read == ["later"]
    assert noted == ["position_lost"]
    assert stub.iterators == [("s-0", LATEST, None), ("s-0", LATEST, None)]
    assert _warnings(caplog) == [f"{WARNING}position_lost"]


@pytest.mark.unit
async def test_a_stream_recreated_under_its_name_restarts_the_shard_at_its_oldest_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = StubStream({"s-0": _Shard(["a1"])})

    def recreate() -> None:
        stub.by_id = {"s-0": _Shard(["b1"])}
        stub.errors["page"] = [_client_error("ResourceNotFoundException")]

    read = await drain(_follow(stub, recreate, nothing, nothing, nothing))

    assert read == ["a1", "b1"]
    assert stub.iterators == [
        ("s-0", TRIM_HORIZON, None),
        ("s-0", AFTER_SEQUENCE_NUMBER, "a1"),
        ("s-0", TRIM_HORIZON, None),
    ]
    assert _warnings(caplog) == [
        f"{WARNING}ClientError:ResourceNotFoundException",
        f"{WARNING}ClientError:InvalidArgumentException",
    ]
