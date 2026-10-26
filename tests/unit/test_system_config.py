"""SystemConfig: per-system YAML holds decisions, validated strictly."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from fire.apps.config import (
    DynamoDbStreamFilter,
    DynamoDbStreamSourceConfig,
    EventSourceConfig,
    HttpProxySourceConfig,
    OtlpSourceConfig,
    ReplaySourceConfig,
    S3CaptureSourceConfig,
    SnsFanoutSourceConfig,
    SystemConfig,
    load_system_config,
    load_system_configs,
)
from fire.core.domain.capture import CaptureEvent
from fire.core.domain.observation import ChannelDirection

SYSTEMS = Path(__file__).resolve().parents[2] / "config" / "systems"
TEMPLATE = SYSTEMS / "_template.yml"
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "data"}
ULID = "[0-7][0-9A-HJKMNP-TV-Z]{25}"


def _minimal(**overrides: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "system": "example",
        "display_name": "Example",
        "notify": {"slack": {"channel_id": "C0123456789"}},
    }
    raw.update(overrides)
    return raw


def _write(directory: Path, name: str, raw: dict[str, Any]) -> Path:
    path = directory / name
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    return path


@pytest.fixture
def acme(tmp_path: Path) -> SystemConfig:
    """The template copied to a systems directory as `acme.yml`, loaded as the body loads it."""
    shutil.copyfile(TEMPLATE, tmp_path / "acme.yml")
    return load_system_configs(tmp_path)["acme"]


def _sources(config: SystemConfig) -> dict[str, Any]:
    return {source.id: source for source in config.sources}


@pytest.mark.unit
def test_template_is_a_valid_system_config() -> None:
    config = SystemConfig.model_validate(yaml.safe_load(TEMPLATE.read_text()))
    assert (config.system, config.display_name) == ("acme", "Acme Shop")
    assert config.notify.slack.channel_id == "C0123456789"
    assert config.notify.slack.channel_name == "#acme-incidents"
    assert config.discovery.manifest is not None
    assert config.discovery.manifest.kind == "event_chains_graph"
    assert config.discovery.manifest.path_env == "ACME_MANIFEST_PATH"
    assert [(source.id, source.kind) for source in config.sources] == [
        ("acme-traces", "otlp"),
        ("acme-api", "http_proxy"),
        ("acme-pricing", "http_proxy"),
        ("domain-events", "kinesis"),
        ("domain-event-items", "dynamodb_stream"),
        ("task-outcomes", "dynamodb_stream"),
        ("inventory-updates", "kinesis"),
        ("shipment-events", "sns_fanout"),
        ("payment-events", "sns_fanout"),
        ("acme-captures", "s3_capture"),
        ("work-unit-samples", "replay"),
    ]
    sources = _sources(config)
    events = sources["domain-events"]
    assert isinstance(events, EventSourceConfig)
    assert (events.location_env, events.direction) == (
        "ACME_EVENTS_STREAM",
        ChannelDirection.INTERNAL,
    )
    assert events.envelope.event_id.path == "id"
    assert events.envelope.work_id is not None and events.envelope.work_id.path == "workUnitId"
    table = sources["domain-event-items"]
    assert isinstance(table, DynamoDbStreamSourceConfig)
    assert (table.location_env, table.filter, table.outcome) == ("ACME_EVENTS_TABLE", None, None)
    assert (table.envelope.event_id.path, table.envelope.event_id.strip_prefix) == ("SK", "EVENT#")
    inbound = sources["inventory-updates"]
    assert isinstance(inbound, EventSourceConfig)
    assert (inbound.location_env, inbound.direction) == (
        "ACME_INVENTORY_UPDATES_STREAM",
        ChannelDirection.INBOUND,
    )
    topics = {
        source_id: (source.location_env, source.raw_delivery)
        for source_id, source in sources.items()
        if isinstance(source, SnsFanoutSourceConfig)
    }
    assert topics == {
        "shipment-events": ("ACME_SHIPMENT_EVENTS_TOPIC", False),
        "payment-events": ("ACME_PAYMENT_EVENTS_TOPIC", False),
    }
    assert config.outcome_sources() == {"task-outcomes"}
    assert [rule.alias for rule in config.operations.http] == ["billing", "shipping"]
    assert all(rule.hosts and rule.paths for rule in config.operations.http)


@pytest.mark.unit
def test_repo_system_configs_are_valid() -> None:
    for system, config in load_system_configs(SYSTEMS).items():
        assert config.system == system


@pytest.mark.unit
def test_the_template_loads_as_a_system_file(acme: SystemConfig) -> None:
    assert acme.source_ids() == {
        "acme-traces",
        "acme-api",
        "acme-pricing",
        "domain-events",
        "domain-event-items",
        "task-outcomes",
        "inventory-updates",
        "shipment-events",
        "payment-events",
        "acme-captures",
        "work-unit-samples",
    }


@pytest.mark.unit
def test_the_template_replays_work_units_through_their_envelope(acme: SystemConfig) -> None:
    replay = _sources(acme)["work-unit-samples"]
    assert isinstance(replay, ReplaySourceConfig)
    assert replay.direction is ChannelDirection.INTERNAL
    assert replay.location_env == "ACME_REPLAY_PATH"
    envelope = replay.envelope
    assert envelope.work_id is not None
    assert envelope.event_id.path == envelope.work_id.path == "work_unit_id"
    assert envelope.occurred_at.path == "metadata.timestamp"
    assert envelope.correlation_id is None


@pytest.mark.unit
def test_minimal_config_defaults() -> None:
    config = SystemConfig.model_validate(_minimal())
    assert config.notify.slack.channel_name is None
    assert config.discovery.manifest is None
    assert config.sources == []
    assert config.operations.http == []
    assert config.policy.critical == []
    assert config.fingerprinting.sample_rate == 1.0


@pytest.mark.unit
def test_slack_channel_id_is_required() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_name": "#x"}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_id", ["C0123456789", "G0ABCDEFGH", "C12345678"])
def test_slack_channel_id_accepts_channel_ids(channel_id: str) -> None:
    config = SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))
    assert config.notify.slack.channel_id == channel_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "channel_id",
    ["#example-incidents", "c0123456789", "D0123456789", "U0123456789", "C1234", ""],
    ids=["name", "lowercase", "dm", "user", "short", "empty"],
)
def test_slack_channel_id_rejects_non_channel_ids(channel_id: str) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": {"channel_id": channel_id}}))


@pytest.mark.unit
@pytest.mark.parametrize("channel_name", ["example-incidents", "#Upper", "#has space"])
def test_slack_channel_name_must_look_like_a_channel(channel_name: str) -> None:
    slack = {"channel_id": "C0123456789", "channel_name": channel_name}
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(notify={"slack": slack}))


@pytest.mark.unit
def test_unknown_keys_are_rejected() -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(team_channel="#old"))


@pytest.mark.unit
def test_critical_and_ignore_must_be_disjoint() -> None:
    with pytest.raises(ValidationError, match="order.created"):
        SystemConfig.model_validate(
            _minimal(policy={"critical": ["order.created"], "ignore": ["order.created"]})
        )


@pytest.mark.unit
@pytest.mark.parametrize("sample_rate", [0, -0.1, 1.5])
def test_sample_rate_is_in_zero_one(sample_rate: float) -> None:
    with pytest.raises(ValidationError):
        SystemConfig.model_validate(_minimal(fingerprinting={"sample_rate": sample_rate}))


DRIFT = {"min_samples": 20, "required_ratio": 1.0}


@pytest.mark.unit
def test_drift_defaults_when_absent() -> None:
    drift = SystemConfig.model_validate(_minimal()).drift
    assert (drift.min_samples, drift.required_ratio) == (20, 1.0)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("drift", "expected"),
    [
        ({"min_samples": 5, "required_ratio": 0.9}, (5, 0.9)),
        ({"min_samples": 5}, (5, 1.0)),
        ({"required_ratio": 0.5}, (20, 0.5)),
        ({}, (20, 1.0)),
    ],
    ids=["both", "min-samples-only", "ratio-only", "empty"],
)
def test_drift_loads_from_yaml(
    tmp_path: Path, drift: dict[str, Any], expected: tuple[int, float]
) -> None:
    config = load_system_config(_write(tmp_path, "example.yml", _minimal(drift=drift)))
    assert (config.drift.min_samples, config.drift.required_ratio) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [("min_samples", 0), ("required_ratio", 0), ("required_ratio", 1.5)],
    ids=["min-samples-zero", "ratio-zero", "ratio-above-one"],
)
def test_drift_rejects_out_of_range_values(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(drift={key: value}))
    assert [error["loc"] for error in caught.value.errors()] == [("drift", key)]


@pytest.mark.unit
def test_drift_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(drift={**DRIFT, "window": 5}))
    assert [error["loc"] for error in caught.value.errors()] == [("drift", "window")]


@pytest.mark.unit
def test_template_declares_a_drift_policy(acme: SystemConfig) -> None:
    assert yaml.safe_load(TEMPLATE.read_text())["drift"] == DRIFT
    assert (acme.drift.min_samples, acme.drift.required_ratio) == (20, 1.0)


PATHS = {"settle_s": 60, "max_open_flows": 10000, "max_spans_per_flow": 1000}


@pytest.mark.unit
def test_paths_default_when_absent() -> None:
    paths = SystemConfig.model_validate(_minimal()).paths
    assert paths.model_dump() == PATHS


@pytest.mark.unit
def test_paths_load_from_yaml(tmp_path: Path) -> None:
    config = load_system_config(
        _write(tmp_path, "example.yml", _minimal(paths={"settle_s": 5, "max_open_flows": 1}))
    )
    assert config.paths.model_dump() == {
        "settle_s": 5,
        "max_open_flows": 1,
        "max_spans_per_flow": 1000,
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [("settle_s", 0.5), ("max_open_flows", 0), ("max_spans_per_flow", 0), ("window_s", 5)],
    ids=["settle-below-one", "open-zero", "spans-zero", "unknown"],
)
def test_paths_reject_values_below_one_and_unknown_keys(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(paths={key: value}))
    assert [error["loc"] for error in caught.value.errors()] == [("paths", key)]


@pytest.mark.unit
def test_the_template_declares_paths_at_their_defaults(acme: SystemConfig) -> None:
    assert yaml.safe_load(TEMPLATE.read_text())["paths"] == PATHS
    assert acme.paths.model_dump() == PATHS


HTTP_ERRORS = {"status_min": 500, "threshold": 5, "window_s": 60}


@pytest.mark.unit
def test_http_errors_is_off_when_absent() -> None:
    assert SystemConfig.model_validate(_minimal()).triggers.http_errors is None
    assert SystemConfig.model_validate(_minimal(triggers={})).triggers.http_errors is None


@pytest.mark.unit
def test_http_errors_loads_from_yaml_with_defaults_for_keys_left_out(tmp_path: Path) -> None:
    config = load_system_config(
        _write(tmp_path, "example.yml", _minimal(triggers={"http_errors": {"threshold": 2}}))
    )
    assert config.triggers.http_errors is not None
    assert config.triggers.http_errors.model_dump() == {**HTTP_ERRORS, "threshold": 2}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("triggers", "loc"),
    [
        ({"http_errors": {"threshold": 0}}, ("triggers", "http_errors", "threshold")),
        ({"http_errors": {"status_min": 600}}, ("triggers", "http_errors", "status_min")),
        ({"http_errors": {"window_s": 0}}, ("triggers", "http_errors", "window_s")),
        ({"http_errors": {**HTTP_ERRORS, "burst": 1}}, ("triggers", "http_errors", "burst")),
        ({"http_5xx": {}}, ("triggers", "http_5xx")),
    ],
    ids=["threshold-zero", "status-above-599", "window-zero", "unknown-key", "unknown-trigger"],
)
def test_http_errors_rejects_out_of_range_values_and_unknown_keys(
    triggers: dict[str, Any], loc: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(triggers=triggers))
    assert [error["loc"] for error in caught.value.errors()] == [loc]


@pytest.mark.unit
def test_the_template_declares_its_http_errors_trigger(acme: SystemConfig) -> None:
    assert yaml.safe_load(TEMPLATE.read_text())["triggers"] == {
        "http_errors": HTTP_ERRORS,
        "dlq": DLQ,
        "correlation": {"window_s": 300},
    }
    http_errors = acme.triggers.http_errors
    assert http_errors is not None
    assert http_errors.model_dump() == HTTP_ERRORS


@pytest.mark.unit
def test_correlation_is_off_when_absent_and_loads_with_its_default(tmp_path: Path) -> None:
    assert SystemConfig.model_validate(_minimal()).triggers.correlation is None
    config = load_system_config(
        _write(tmp_path, "example.yml", _minimal(triggers={"correlation": {}}))
    )
    assert config.triggers.correlation is not None
    assert config.triggers.correlation.model_dump() == {"window_s": 300}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("correlation", "loc"),
    [
        ({"window_s": 0}, ("triggers", "correlation", "window_s")),
        ({"window_s": -1}, ("triggers", "correlation", "window_s")),
        ({"window_s": float("inf")}, ("triggers", "correlation", "window_s")),
        ({"window_s": 60, "max_s": 600}, ("triggers", "correlation", "max_s")),
    ],
    ids=["zero", "negative", "infinite", "unknown-key"],
)
def test_correlation_rejects_a_window_that_is_not_positive_and_finite_and_unknown_keys(
    correlation: dict[str, Any], loc: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(triggers={"correlation": correlation}))
    assert [error["loc"] for error in caught.value.errors()] == [loc]


@pytest.mark.unit
def test_the_template_correlates_for_longer_than_its_flows_settle(acme: SystemConfig) -> None:
    assert acme.triggers.correlation is not None
    assert acme.triggers.correlation.window_s == 300 > acme.paths.settle_s


DLQ = {"min_abandoned": 3, "min_rate": 0.2}
WORK_ENVELOPE = {**ENVELOPE, "work_id": "id"}


def _outcomes(**overrides: Any) -> dict[str, Any]:
    return {
        "id": "outcomes",
        "kind": "replay",
        "direction": "internal",
        "path_env": "O",
        "outcome": "abandoned",
        "envelope": WORK_ENVELOPE,
        **overrides,
    }


@pytest.mark.unit
def test_dlq_is_off_when_absent_and_loads_with_defaults(tmp_path: Path) -> None:
    assert SystemConfig.model_validate(_minimal()).triggers.dlq is None
    config = load_system_config(
        _write(
            tmp_path,
            "example.yml",
            _minimal(sources=[_outcomes()], triggers={"dlq": {"min_rate": 0.5}}),
        )
    )
    assert config.triggers.dlq is not None
    assert config.triggers.dlq.model_dump() == {**DLQ, "min_rate": 0.5}
    assert config.outcome_sources() == {"outcomes"}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("dlq", "loc"),
    [
        ({"min_abandoned": 0}, ("triggers", "dlq", "min_abandoned")),
        ({"min_rate": 0}, ("triggers", "dlq", "min_rate")),
        ({"min_rate": 1.5}, ("triggers", "dlq", "min_rate")),
        ({"min_rate": float("nan")}, ("triggers", "dlq", "min_rate")),
        ({**DLQ, "window_s": 60}, ("triggers", "dlq", "window_s")),
    ],
    ids=["abandoned-zero", "rate-zero", "rate-above-one", "rate-nan", "unknown-key"],
)
def test_dlq_rejects_out_of_range_values_and_unknown_keys(
    dlq: dict[str, Any], loc: tuple[str, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(sources=[_outcomes()], triggers={"dlq": dlq}))
    assert [error["loc"] for error in caught.value.errors()] == [loc]


@pytest.mark.unit
def test_dlq_needs_a_source_with_outcome_abandoned() -> None:
    with pytest.raises(ValidationError, match="triggers.dlq needs a source with outcome"):
        SystemConfig.model_validate(
            _minimal(sources=[_source("replay", envelope=WORK_ENVELOPE)], triggers={"dlq": {}})
        )


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["replay", "kinesis", "dynamodb_stream", "sns_fanout"])
def test_every_event_kind_takes_an_outcome_that_names_its_work(kind: str) -> None:
    [source] = SystemConfig.model_validate(
        _minimal(sources=[_source(kind, outcome="abandoned", envelope=WORK_ENVELOPE)])
    ).sources
    assert isinstance(source, EventSourceConfig)
    assert source.outcome == "abandoned"
    with pytest.raises(ValidationError, match="an outcome source needs envelope.work_id"):
        SystemConfig.model_validate(_minimal(sources=[_source(kind, outcome="abandoned")]))
    assert _source_error_locs(_source(kind, outcome="failed", envelope=WORK_ENVELOPE)) == [
        ("sources", 0, kind, "outcome")
    ]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["s3_capture", "http_proxy", "otlp"])
def test_kinds_without_an_envelope_take_no_outcome(kind: str) -> None:
    assert _source_error_locs(_source(kind, outcome="abandoned")) == [
        ("sources", 0, kind, "outcome")
    ]


@pytest.mark.unit
def test_the_template_reads_abandoned_outbox_rows_and_declares_the_dlq_trigger(
    acme: SystemConfig,
) -> None:
    outcomes = _sources(acme)["task-outcomes"]
    assert isinstance(outcomes, DynamoDbStreamSourceConfig)
    assert (outcomes.location_env, outcomes.outcome, outcomes.direction) == (
        "ACME_OUTBOX_TABLE",
        "abandoned",
        ChannelDirection.INTERNAL,
    )
    assert outcomes.filter == DynamoDbStreamFilter(attribute="status", equals="ABANDONED")
    envelope = outcomes.envelope
    assert envelope.work_id is not None
    assert envelope.event_id.path == envelope.work_id.path == "work_unit_id"
    assert (envelope.event_type.path, envelope.payload.path) == ("processor_name", "error_details")
    assert (envelope.occurred_at.path, envelope.occurred_at.epoch) == ("timestamp", "ms")
    assert acme.outcome_sources() == {"task-outcomes"}
    assert acme.triggers.dlq is not None and acme.triggers.dlq.model_dump() == DLQ


CAPTURE = {"prefix": "partner-payloads/", "label": "partner", "layout": "work_unit_action"}

# One valid source per kind, with the env var that names what it reads.
SOURCES: dict[str, tuple[dict[str, Any], str]] = {
    "replay": (
        {"kind": "replay", "direction": "internal", "path_env": "P", "envelope": ENVELOPE},
        "P",
    ),
    "kinesis": (
        {
            "kind": "kinesis",
            "direction": "internal",
            "stream_env": "S",
            "regions": ["us-east-1"],
            "envelope": ENVELOPE,
        },
        "S",
    ),
    "dynamodb_stream": (
        {
            "kind": "dynamodb_stream",
            "direction": "internal",
            "table_env": "T",
            "regions": ["us-east-1"],
            "filter": {"attribute": "status", "equals": "ABANDONED"},
            "envelope": ENVELOPE,
        },
        "T",
    ),
    "sns_fanout": (
        {
            "kind": "sns_fanout",
            "direction": "inbound",
            "topic_env": "N",
            "regions": ["us-east-1"],
            "envelope": ENVELOPE,
        },
        "N",
    ),
    "s3_capture": ({"kind": "s3_capture", "bucket_env": "B", "captures": [CAPTURE]}, "B"),
    "http_proxy": ({"kind": "http_proxy", "listen_port": 8021, "upstream_env": "U"}, "U"),
    "otlp": ({"kind": "otlp", "listen_env": "L"}, "L"),
}


def _source(kind: str = "kinesis", **overrides: Any) -> dict[str, Any]:
    source: dict[str, Any] = {"id": "events", **SOURCES[kind][0]}
    source.update(overrides)
    return source


def _source_error_locs(*sources: dict[str, Any]) -> list[tuple[str | int, ...]]:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(sources=list(sources)))
    return [error["loc"] for error in caught.value.errors()]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_each_source_kind_loads_and_names_its_env_var(kind: str) -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source(kind)])).sources
    assert source.kind == kind
    assert source.location_env == SOURCES[kind][1]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_source_rejects_unknown_keys(kind: str) -> None:
    assert _source_error_locs(_source(kind, extra="x")) == [("sources", 0, kind, "extra")]


@pytest.mark.unit
def test_unknown_source_kind_is_rejected() -> None:
    assert _source_error_locs({**_source(), "kind": "kafka"}) == [("sources", 0)]


@pytest.mark.unit
@pytest.mark.parametrize("kind", sorted(SOURCES))
def test_dedupe_key_fails_naming_envelope_event_id(kind: str) -> None:
    with pytest.raises(ValidationError, match=r"dedupe_key was removed.*envelope\.event_id"):
        SystemConfig.model_validate(_minimal(sources=[_source(kind, dedupe_key="id")]))


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["kinesis", "dynamodb_stream", "sns_fanout"])
def test_regional_source_needs_a_region(kind: str) -> None:
    assert _source_error_locs(_source(kind, regions=[])) == [("sources", 0, kind, "regions")]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["kinesis", "dynamodb_stream", "sns_fanout"])
@pytest.mark.parametrize(
    "regions",
    [["us-east-1", "us-east-1"], ["US East"], [""]],
    ids=["twice", "not-a-name", "empty"],
)
def test_regions_are_distinct_region_names(kind: str, regions: list[str]) -> None:
    locs = _source_error_locs(_source(kind, regions=regions))
    assert locs and all(loc[:4] == ("sources", 0, kind, "regions") for loc in locs)


@pytest.mark.unit
def test_event_source_reading_defaults() -> None:
    sources = SystemConfig.model_validate(
        _minimal(sources=[_source(kind, id=kind) for kind in ("kinesis", "dynamodb_stream")])
    ).sources
    assert [source.poll_s for source in sources] == [1, 1]  # type: ignore[union-attr]
    [fanout] = SystemConfig.model_validate(_minimal(sources=[_source("sns_fanout")])).sources
    assert isinstance(fanout, SnsFanoutSourceConfig)
    assert (fanout.raw_delivery, fanout.wait_s, fanout.visibility_s, fanout.poll_s) == (
        False,
        5,
        300,
        1,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kind", "key", "value"),
    [
        ("kinesis", "poll_s", 0),
        ("dynamodb_stream", "poll_s", 0),
        ("sns_fanout", "poll_s", 0),
        ("sns_fanout", "wait_s", 21),
        ("sns_fanout", "wait_s", -1),
        ("sns_fanout", "visibility_s", 43201),
        ("sns_fanout", "visibility_s", 29),
        ("sns_fanout", "visibility_s", 0),
        ("sns_fanout", "raw_delivery", "sometimes"),
    ],
)
def test_event_source_reading_keys_are_checked(kind: str, key: str, value: Any) -> None:
    assert _source_error_locs(_source(kind, **{key: value})) == [("sources", 0, kind, key)]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["replay", "kinesis", "dynamodb_stream", "sns_fanout"])
@pytest.mark.parametrize("key", ["direction", "envelope"])
def test_event_source_needs_direction_and_envelope(kind: str, key: str) -> None:
    source = _source(kind)
    del source[key]
    assert _source_error_locs(source) == [("sources", 0, kind, key)]


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["replay", "kinesis", "dynamodb_stream", "sns_fanout"])
def test_event_source_is_never_outbound(kind: str) -> None:
    locs = _source_error_locs(_source(kind, direction="outbound"))
    assert locs == [("sources", 0, kind, "direction")]


@pytest.mark.unit
@pytest.mark.parametrize("key", ["direction", "envelope"])
def test_s3_capture_takes_no_direction_or_envelope(key: str) -> None:
    value = ENVELOPE if key == "envelope" else "outbound"
    locs = _source_error_locs(_source("s3_capture", **{key: value}))
    assert locs == [("sources", 0, "s3_capture", key)]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("capture", "loc"),
    [
        ({**CAPTURE, "label": "Partner"}, ("captures", 0, "label")),
        ({**CAPTURE, "layout": "by_order"}, ("captures", 0, "layout")),
        ({**CAPTURE, "prefix": ""}, ("captures", 0, "prefix")),
        ({**CAPTURE, "order_id": "x"}, ("captures", 0, "order_id")),
    ],
    ids=["label", "layout", "prefix", "unknown-key"],
)
def test_s3_capture_checks_each_capture(capture: dict[str, Any], loc: tuple[Any, ...]) -> None:
    locs = _source_error_locs(_source("s3_capture", captures=[capture]))
    assert locs == [("sources", 0, "s3_capture", *loc)]


@pytest.mark.unit
def test_s3_capture_needs_unique_prefixes() -> None:
    captures = [CAPTURE, {**CAPTURE, "label": "other"}]
    with pytest.raises(ValidationError, match="duplicate capture prefixes: partner-payloads/"):
        SystemConfig.model_validate(_minimal(sources=[_source("s3_capture", captures=captures)]))


@pytest.mark.unit
def test_s3_capture_needs_a_capture() -> None:
    locs = _source_error_locs(_source("s3_capture", captures=[]))
    assert locs == [("sources", 0, "s3_capture", "captures")]


@pytest.mark.unit
def test_s3_capture_layouts() -> None:
    layouts = ["work_unit_action", "work_unit_entity_action", "work_unit_entity"]
    captures = [
        {"prefix": f"{layout}/", "label": "partner", "layout": layout} for layout in layouts
    ]
    [source] = SystemConfig.model_validate(
        _minimal(sources=[_source("s3_capture", captures=captures)])
    ).sources
    assert isinstance(source, S3CaptureSourceConfig)
    assert [capture.layout for capture in source.captures] == layouts


@pytest.mark.unit
def test_s3_capture_defaults() -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source("s3_capture")])).sources
    assert isinstance(source, S3CaptureSourceConfig)
    assert (source.poll_s, source.settle_s) == (5, 10)
    assert source.max_object_bytes == 1_048_576
    assert source.work_id_pattern is None


@pytest.mark.unit
def test_s3_capture_reading_keys_load() -> None:
    reading = {"poll_s": 0.05, "settle_s": 0, "max_object_bytes": 1, "work_id_pattern": ULID}
    [source] = SystemConfig.model_validate(
        _minimal(sources=[_source("s3_capture", **reading)])
    ).sources
    assert isinstance(source, S3CaptureSourceConfig)
    assert source.model_dump(include=set(reading)) == reading


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("poll_s", 0),
        ("settle_s", -1),
        ("max_object_bytes", 0),
        ("work_id_pattern", "[0-7"),
        ("work_id_pattern", ""),
    ],
    ids=["poll-zero", "settle-negative", "max-bytes-zero", "pattern-no-regex", "pattern-empty"],
)
def test_s3_capture_checks_its_reading_keys(key: str, value: Any) -> None:
    locs = _source_error_locs(_source("s3_capture", **{key: value}))
    assert locs == [("sources", 0, "s3_capture", key)]


@pytest.mark.unit
def test_s3_capture_names_a_pattern_that_is_no_regular_expression() -> None:
    with pytest.raises(ValidationError, match="work_id_pattern is not a regular expression"):
        SystemConfig.model_validate(
            _minimal(sources=[_source("s3_capture", work_id_pattern="(unclosed")])
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    "prefix",
    ["partner-payloads", "/partner-payloads/", "partner payloads/", "/"],
    ids=["no-trailing-slash", "leading-slash", "space", "slash-only"],
)
def test_s3_capture_prefix_ends_with_a_slash_and_starts_with_none(prefix: str) -> None:
    locs = _source_error_locs(_source("s3_capture", captures=[{**CAPTURE, "prefix": prefix}]))
    assert locs == [("sources", 0, "s3_capture", "captures", 0, "prefix")]


@pytest.mark.unit
def test_s3_capture_prefixes_lie_inside_no_other_prefix() -> None:
    captures = [
        {**CAPTURE, "prefix": "a/b/", "label": "inner"},
        {**CAPTURE, "prefix": "a/", "label": "outer"},
        {**CAPTURE, "prefix": "ab/", "label": "beside"},
    ]
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(sources=[_source("s3_capture", captures=captures)]))
    assert [(error["loc"], error["msg"]) for error in caught.value.errors()] == [
        (
            ("sources", 0, "s3_capture"),
            "Value error, capture prefixes inside another prefix: a/b/",
        )
    ]


@pytest.mark.unit
def test_the_template_reads_its_captures_with_the_ulid_work_id_pattern(
    acme: SystemConfig,
) -> None:
    captures = _sources(acme)["acme-captures"]
    assert isinstance(captures, S3CaptureSourceConfig)
    assert captures.location_env == "ACME_CAPTURE_BUCKET"
    assert captures.work_id_pattern == ULID
    assert [(capture.prefix, capture.label, capture.layout) for capture in captures.captures] == [
        ("notification-payloads/", "notifications", "work_unit_action"),
        ("shipping-payloads/", "shipping", "work_unit_entity_action"),
        ("billing-payloads/", "billing", "work_unit_entity"),
    ]


@pytest.mark.unit
def test_http_proxy_defaults() -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source("http_proxy")])).sources
    assert isinstance(source, HttpProxySourceConfig)
    assert (source.listen_host, source.listen_port) == ("127.0.0.1", 8021)
    assert source.openapi_path == "/openapi.json"
    assert source.openapi_refresh_s == 300
    assert source.max_body_bytes == 1_048_576
    assert source.openapi_file_env is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("listen_port", 0),
        ("listen_port", 65536),
        ("listen_host", ""),
        ("openapi_path", "openapi.json"),
        ("openapi_path", "/open api.json"),
        ("openapi_refresh_s", 0),
        ("max_body_bytes", 0),
        ("openapi_file_env", ""),
        ("direction", "inbound"),
        ("envelope", ENVELOPE),
    ],
)
def test_http_proxy_checks_its_keys(key: str, value: Any) -> None:
    locs = _source_error_locs(_source("http_proxy", **{key: value}))
    assert locs == [("sources", 0, "http_proxy", key)]


@pytest.mark.unit
@pytest.mark.parametrize("key", ["listen_port", "upstream_env"])
def test_http_proxy_needs_a_port_and_an_upstream(key: str) -> None:
    source = _source("http_proxy")
    del source[key]
    assert _source_error_locs(source) == [("sources", 0, "http_proxy", key)]


@pytest.mark.unit
def test_http_proxies_need_unique_listen_addresses() -> None:
    sources = [
        _source("http_proxy", id="api"),
        _source("http_proxy", id="other-host", listen_host="0.0.0.0"),
        _source("http_proxy", id="same", upstream_env="V"),
    ]
    with pytest.raises(ValidationError, match="duplicate listen addresses: 127.0.0.1:8021"):
        SystemConfig.model_validate(_minimal(sources=sources))


@pytest.mark.unit
def test_the_template_proxies_sit_in_front_of_the_api_and_the_pricing_service(
    acme: SystemConfig,
) -> None:
    sources = _sources(acme)
    proxies = {
        source_id: (source.listen_port, source.location_env, source.openapi_file_env)
        for source_id, source in sources.items()
        if isinstance(source, HttpProxySourceConfig)
    }
    assert proxies == {
        "acme-api": (8180, "ACME_API_UPSTREAM", "ACME_API_OPENAPI_PATH"),
        "acme-pricing": (8190, "ACME_PRICING_UPSTREAM", "ACME_PRICING_OPENAPI_PATH"),
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("listen_env", ""),
        ("listen_port", 4318),
        ("listen_host", "0.0.0.0"),
        ("direction", "inbound"),
        ("envelope", ENVELOPE),
    ],
)
def test_otlp_checks_its_keys(key: str, value: Any) -> None:
    locs = _source_error_locs(_source("otlp", **{key: value}))
    assert locs == [("sources", 0, "otlp", key)]


CAPTURE_EVENT = {
    "name": "capture.written",
    "request_uri": "acme.capture.request_uri",
    "response_uri": "acme.capture.response_uri",
}


@pytest.mark.unit
def test_otlp_capture_event_is_optional() -> None:
    [source] = SystemConfig.model_validate(_minimal(sources=[_source("otlp")])).sources
    assert isinstance(source, OtlpSourceConfig)
    assert source.capture_event is None


@pytest.mark.unit
@pytest.mark.parametrize(("ttl", "expected"), [({}, 900), ({"ttl_s": 60.5}, 60.5)])
def test_otlp_capture_event_loads_with_its_ttl(ttl: dict[str, Any], expected: float) -> None:
    [source] = SystemConfig.model_validate(
        _minimal(sources=[_source("otlp", capture_event={**CAPTURE_EVENT, **ttl})])
    ).sources
    assert isinstance(source, OtlpSourceConfig)
    assert source.capture_event == CaptureEvent(**CAPTURE_EVENT, ttl_s=expected)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("capture_event", "keys"),
    [
        ({"request_uri": "a", "response_uri": "b"}, [("name",)]),
        ({**CAPTURE_EVENT, "name": ""}, [("name",)]),
        ({"name": "capture.written"}, [("request_uri",), ("response_uri",)]),
        ({**CAPTURE_EVENT, "response_uri": CAPTURE_EVENT["request_uri"]}, [()]),
        ({**CAPTURE_EVENT, "ttl_s": 0}, [("ttl_s",)]),
        ({**CAPTURE_EVENT, "status_code": "acme.capture.status_code"}, [("status_code",)]),
    ],
    ids=["missing-name", "empty-name", "missing-uris", "same-uris", "ttl-zero", "unknown-key"],
)
def test_otlp_capture_event_errors_name_their_location(
    capture_event: dict[str, Any], keys: list[tuple[str, ...]]
) -> None:
    locs = _source_error_locs(_source("otlp", capture_event=capture_event))
    assert locs == [("sources", 0, "otlp", "capture_event", *key) for key in keys]


@pytest.mark.unit
def test_otlp_capture_event_needs_two_different_attributes() -> None:
    same = {**CAPTURE_EVENT, "response_uri": CAPTURE_EVENT["request_uri"]}
    with pytest.raises(ValidationError, match="request_uri and response_uri must be different"):
        SystemConfig.model_validate(_minimal(sources=[_source("otlp", capture_event=same)]))


@pytest.mark.unit
def test_otlp_needs_a_listen_env() -> None:
    source = _source("otlp")
    del source["listen_env"]
    assert _source_error_locs(source) == [("sources", 0, "otlp", "listen_env")]


@pytest.mark.unit
def test_the_template_receives_its_collectors_spans(acme: SystemConfig) -> None:
    traces = _sources(acme)["acme-traces"]
    assert isinstance(traces, OtlpSourceConfig)
    assert traces.location_env == "ACME_OTLP_LISTEN"
    assert traces.capture_event == CaptureEvent(**CAPTURE_EVENT, ttl_s=900)


@pytest.mark.unit
def test_operations_load_from_the_system_yaml() -> None:
    rule = {"hosts": ["api.partner.example.com"], "alias": "partner", "paths": ["/v1/{id}"]}
    config = SystemConfig.model_validate(_minimal(operations={"http": [rule]}))
    assert config.operations.http[0].paths == ["/v1/{id}"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("operations", "loc"),
    [
        ({"grpc": []}, ("operations", "grpc")),
        ({"http": [{"hosts": ["a.example"], "alias": "A"}]}, ("operations", "http", 0, "alias")),
        (
            {"http": [{"hosts": ["a.example"], "alias": "a", "paths": ["/x", "/x"]}]},
            ("operations", "http", 0, "paths"),
        ),
    ],
    ids=["unknown-key", "alias", "duplicate-template"],
)
def test_operations_errors_name_their_location(
    operations: dict[str, Any], loc: tuple[str | int, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(operations=operations))
    assert [error["loc"] for error in caught.value.errors()] == [loc]


@pytest.mark.unit
def test_source_id_is_required() -> None:
    source = _source()
    del source["id"]
    assert _source_error_locs(source) == [("sources", 0, "kinesis", "id")]


@pytest.mark.unit
@pytest.mark.parametrize("source_id", ["domain-events", "inventory_updates", "0", "a1"])
def test_source_id_accepts_slugs(source_id: str) -> None:
    config = SystemConfig.model_validate(_minimal(sources=[_source(id=source_id)]))
    assert config.source_ids() == {source_id}


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_id",
    ["", "-events", "_events", "Events", "domain events", "domain.events", "events/1"],
    ids=["empty", "leading-dash", "leading-underscore", "uppercase", "space", "dot", "slash"],
)
def test_source_id_rejects_non_slugs(source_id: str) -> None:
    assert _source_error_locs(_source(id=source_id)) == [("sources", 0, "kinesis", "id")]


@pytest.mark.unit
def test_source_ids_are_unique_within_a_system() -> None:
    sources = [_source(id="b"), _source("replay", id="a"), _source(id="b"), _source(id="a")]
    with pytest.raises(ValidationError, match="duplicate source ids: a, b"):
        SystemConfig.model_validate(_minimal(sources=sources))


@pytest.mark.unit
def test_source_ids_may_repeat_across_systems() -> None:
    first = SystemConfig.model_validate(_minimal(sources=[_source()]))
    second = SystemConfig.model_validate(_minimal(system="other", sources=[_source()]))
    assert first.source_ids() == second.source_ids() == {"events"}


@pytest.mark.unit
def test_file_name_must_match_system(tmp_path: Path) -> None:
    path = _write(tmp_path, "other.yml", _minimal())
    with pytest.raises(ValueError, match="must match the file name"):
        load_system_config(path)


@pytest.mark.unit
def test_load_system_configs_skips_underscore_files(tmp_path: Path) -> None:
    _write(tmp_path, "example.yml", _minimal())
    _write(tmp_path, "_template.yml", _minimal(system="template"))
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")

    assert list(load_system_configs(tmp_path)) == ["example"]


@pytest.mark.unit
def test_load_system_configs_missing_directory(tmp_path: Path) -> None:
    assert load_system_configs(tmp_path / "absent") == {}


@pytest.mark.unit
def test_triggers_default_to_none() -> None:
    assert SystemConfig.model_validate(_minimal()).triggers.incident_io is None


INCIDENT_IO = {
    "match": [
        {
            "custom_field_id": "01FCNDV6P870EA6S7TK1DSYDG0",
            "values": ["01FCNDV6P870EA6S7TK1DSYDG1"],
        },
        {"team_id": "01JPQA75EPNEES4479P16P4XAB"},
        {"incident_type_id": "01FCNDV6P870EA6S7TK1DSYDG2"},
    ]
}


@pytest.mark.unit
def test_incident_io_maps_incidents_by_field_by_team_and_by_type(tmp_path: Path) -> None:
    config = load_system_config(
        _write(tmp_path, "example.yml", _minimal(triggers={"incident_io": INCIDENT_IO}))
    )
    trigger = config.triggers.incident_io
    assert trigger is not None
    by_field, by_team, by_type = trigger.match
    assert (by_field.custom_field_id, by_field.values) == (
        "01FCNDV6P870EA6S7TK1DSYDG0",
        ["01FCNDV6P870EA6S7TK1DSYDG1"],
    )
    assert by_team.team_id == "01JPQA75EPNEES4479P16P4XAB"
    assert by_type.incident_type_id == "01FCNDV6P870EA6S7TK1DSYDG2"


@pytest.mark.unit
def test_the_template_maps_no_incident_io_incident_until_its_block_is_uncommented(
    acme: SystemConfig,
) -> None:
    assert acme.triggers.incident_io is None


@pytest.mark.unit
def test_the_template_incident_io_block_loads_once_uncommented_with_ids() -> None:
    text = TEMPLATE.read_text()
    head, marker, block = text.partition("  # incident_io:\n")
    assert marker
    uncommented = "".join(
        "  " + line.removeprefix("  # ") for line in (marker + block).splitlines(keepends=True)
    )
    for placeholder, value in (
        ("<field id>", "01FCNDV6P870EA6S7TK1DSYDG0"),
        ("<catalog entry id>", "01FCNDV6P870EA6S7TK1DSYDG1"),
        ("<team id>", "01JPQA75EPNEES4479P16P4XAB"),
        ("<incident type id>", "01FCNDV6P870EA6S7TK1DSYDG2"),
    ):
        uncommented = uncommented.replace(placeholder, value)
    trigger = SystemConfig.model_validate(yaml.safe_load(head + uncommented)).triggers.incident_io
    assert trigger is not None
    assert [
        (rule.custom_field_id, rule.values, rule.team_id, rule.incident_type_id)
        for rule in trigger.match
    ] == [
        ("01FCNDV6P870EA6S7TK1DSYDG0", ["01FCNDV6P870EA6S7TK1DSYDG1"], None, None),
        (None, [], "01JPQA75EPNEES4479P16P4XAB", None),
        (None, [], None, "01FCNDV6P870EA6S7TK1DSYDG2"),
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("triggers", "loc"),
    [
        ({"pagerduty": {}}, ("triggers", "pagerduty")),
        ({"incident_io": {}}, ("triggers", "incident_io", "match")),
        (
            {"incident_io": {"match": [{"team_id": 1}]}},
            ("triggers", "incident_io", "match", 0, "team_id"),
        ),
        ({"incident_io": {"match": [{}]}}, ("triggers", "incident_io", "match", 0)),
    ],
    ids=["unknown-trigger", "no-match", "unquoted-numeric-id", "no-selector"],
)
def test_triggers_errors_name_their_location(
    triggers: dict[str, Any], loc: tuple[Any, ...]
) -> None:
    with pytest.raises(ValidationError) as caught:
        SystemConfig.model_validate(_minimal(triggers=triggers))
    assert [error["loc"] for error in caught.value.errors()] == [loc]
