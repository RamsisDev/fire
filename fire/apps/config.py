"""Load YAML config. Credential values stay as env var *names* until wiring."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.capture import CaptureEvent, CaptureRule
from fire.core.domain.correlation import CorrelationPolicy
from fire.core.domain.dlq import DlqTrigger
from fire.core.domain.envelope import DottedPath, Envelope
from fire.core.domain.flow import PathPolicy
from fire.core.domain.http_errors import HttpErrorsTrigger
from fire.core.domain.incident_io import IncidentIoTrigger
from fire.core.domain.observation import ChannelDirection
from fire.core.domain.operation import OperationRules


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ApiConfig(BaseModel):
    token_env: str = "FIRE_API_TOKEN"


class ReasonerConfig(_Strict):
    provider: str = "echo"
    # Read by the `laya` provider only: System One over Jev's `POST /v1/systemone`.
    # Holds the full URL of that endpoint; unset fails startup.
    base_url_env: str = Field(default="FIRE_SYSTEMONE_BASE_URL", min_length=1)
    # Holds an optional bearer token (LAYA_API_KEY on the server).
    api_key_env: str = Field(default="FIRE_SYSTEMONE_API_KEY", min_length=1)
    # Checkpoint name, such as a LAYA_EXTRA_MODELS name; unset, the server's default.
    model: str | None = None
    timeout_s: float = Field(default=10, gt=0)
    # A `sufficient` answer below this makes the decision noul.
    min_sufficient: float = Field(default=0.5, ge=0, le=1)


class NarratorConfig(BaseModel):
    provider: str = "template"
    base_url_env: str = "FIRE_NARRATOR_BASE_URL"
    api_key_env: str = "FIRE_NARRATOR_API_KEY"
    model_env: str = "FIRE_NARRATOR_MODEL"


class NotifierConfig(_Strict):
    provider: str = "console"
    # Read by the `slack` provider only.
    bot_token_env: str = Field(default="FIRE_SLACK_BOT_TOKEN", min_length=1)
    timeout_s: float = Field(default=5, gt=0)


class BudgetsConfig(BaseModel):
    tier0_ms: int = 300
    tier1_ms: int = 3000
    tier2_ms: int = 1500


class RetryConfig(_Strict):
    # The first backoff doubles per attempt up to 30 s, with full jitter.
    max_attempts: int = Field(default=6, ge=1)
    backoff_ms: int = Field(default=500, ge=0)


class TentacleConfig(_Strict):
    """How the tentacle sends observations to the body. Contract: docs/specs/tentacle.md."""

    body_url_env: str = Field(default="FIRE_BODY_URL", min_length=1)
    batch_size: int = Field(default=200, ge=1)
    flush_ms: int = Field(default=1000, ge=1)
    buffer_size: int = Field(default=10000, ge=1)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    shutdown_s: float = Field(default=10, gt=0)


class CatalogConfig(_Strict):
    """Where the body keeps observations, baselines, and paths. Contract:
    docs/specs/observations.md#observationcatalog.
    """

    provider: Literal["memory", "sqlite"] = "memory"
    # Holds the SQLite file's path; read only by the `sqlite` provider.
    path_env: str = Field(default="FIRE_CATALOG_PATH", min_length=1)
    # How long a dedupe key and a work index entry are kept after acceptance: seven days.
    retention_s: float = Field(default=604800, gt=0, allow_inf_nan=False)


class FeedbackConfig(_Strict):
    """Where the body keeps decisions and the human verdicts on them, queued for retraining."""

    provider: Literal["memory", "sqlite"] = "memory"
    # Holds the SQLite file's path; read only by the `sqlite` provider.
    path_env: str = Field(default="FIRE_FEEDBACK_PATH", min_length=1)


class RetrainingConfig(_Strict):
    """`python -m fire.apps.retrain`: exports pending feedback and runs the trainer."""

    # Holds the checkpoint `train_command` starts from.
    base_checkpoint_env: str = Field(default="FIRE_LAYA_BASE_CHECKPOINT", min_length=1)
    # Holds the directory for batches and checkpoints; unset, var/retrain.
    output_dir_env: str = Field(default="FIRE_LAYA_RETRAIN_DIR", min_length=1)
    # `train` refuses below this many pending rows.
    min_feedback: int = Field(default=20, ge=1)
    train_command: str = Field(default="laya-train", min_length=1)
    epochs: int = Field(default=8, ge=1)


class IncidentIoWebhookConfig(_Strict):
    """`POST /v0/webhooks/incident-io`. Contract:
    docs/specs/ingest-drift-and-feedback.md#incidentio-webhook.
    """

    # Holds the endpoint's signing secret, whsec_ and base64, from incident.io's webhook settings.
    signing_secret_env: str = Field(default="FIRE_INCIDENT_IO_SIGNING_SECRET", min_length=1)
    # Largest gap, either way, between webhook-timestamp and the body's clock.
    tolerance_s: int = Field(default=300, ge=1)


class WebhooksConfig(_Strict):
    incident_io: IncidentIoWebhookConfig = Field(default_factory=IncidentIoWebhookConfig)


class FireConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    reasoner: ReasonerConfig = Field(default_factory=ReasonerConfig)
    narrator: NarratorConfig = Field(default_factory=NarratorConfig)
    notifier: NotifierConfig = Field(default_factory=NotifierConfig)
    budgets: BudgetsConfig = Field(default_factory=BudgetsConfig)
    tentacle: TentacleConfig = Field(default_factory=TentacleConfig)
    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    feedback: FeedbackConfig = Field(default_factory=FeedbackConfig)
    retraining: RetrainingConfig = Field(default_factory=RetrainingConfig)


def load_fire_config(path: Path) -> FireConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    return FireConfig.model_validate(raw)


# Per-system config: what a person decides. What the system contains is discovered, never
# listed here. Contract: docs/specs/system-config-and-discovery.md.


class SlackTarget(_Strict):
    # Public channel ids start with C, private ones with G. The id survives channel renames.
    channel_id: str = Field(pattern=r"^[CG][A-Z0-9]{8,}$")
    channel_name: str | None = Field(default=None, pattern=r"^#[a-z0-9_-]{1,80}$")


class NotifyConfig(_Strict):
    slack: SlackTarget


class ManifestConfig(_Strict):
    kind: str = Field(min_length=1)
    path_env: str = Field(min_length=1)


class DiscoveryConfig(_Strict):
    manifest: ManifestConfig | None = None


# Sources: where the tentacle reads. Contract: docs/specs/tentacle.md#sources-in-the-system-yaml.

Slug = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")]
EnvName = Annotated[str, Field(min_length=1)]


def _distinct(regions: list[str]) -> list[str]:
    repeated = sorted({region for region in regions if regions.count(region) > 1})
    if repeated:
        raise ValueError(f"regions listed twice: {', '.join(repeated)}")
    return regions


# Read in parallel; a record's region is the one it was read from.
Regions = Annotated[
    list[Annotated[str, Field(pattern=r"^[a-z0-9-]+$")]],
    Field(min_length=1),
    AfterValidator(_distinct),
]


class _Source(_Strict):
    # The `source` of every observation read from it; unique within the system.
    id: Slug

    @model_validator(mode="before")
    @classmethod
    def _dedupe_key_was_removed(cls, data: Any) -> Any:
        if isinstance(data, dict) and "dedupe_key" in data:
            raise ValueError("dedupe_key was removed: declare envelope.event_id instead")
        return data

    @property
    def location_env(self) -> str:
        """The env var naming what the source reads: a path, stream, table, topic, bucket,
        upstream, or listen address.
        """
        raise NotImplementedError


class EventSourceConfig(_Source):
    """A source whose records carry events, read through an envelope."""

    direction: ChannelDirection
    envelope: Envelope
    # Its records are the system's abandoned work units, not events: each marks its work_id
    # abandoned. Contract: docs/specs/ingest-drift-and-feedback.md#dlq-attribution.
    outcome: Literal["abandoned"] | None = None

    @field_validator("direction")
    @classmethod
    def _events_enter_or_stay(cls, direction: ChannelDirection) -> ChannelDirection:
        if direction is ChannelDirection.OUTBOUND:
            raise ValueError("an event source is inbound or internal")
        return direction

    @model_validator(mode="after")
    def _outcomes_name_their_work(self) -> EventSourceConfig:
        if self.outcome is not None and self.envelope.work_id is None:
            raise ValueError("an outcome source needs envelope.work_id")
        return self


class ReplaySourceConfig(EventSourceConfig):
    kind: Literal["replay"]
    # A .json file (an object or a list), a .jsonl file, or a directory of them.
    path_env: EnvName

    @property
    def location_env(self) -> str:
        return self.path_env


class KinesisSourceConfig(EventSourceConfig):
    """Every record of a Kinesis data stream. Contract: docs/specs/tentacle.md#kinds."""

    kind: Literal["kinesis"]
    # Holds the stream name, the same in every region.
    stream_env: EnvName
    regions: Regions
    # Seconds between passes over the shards, one GetRecords per shard each.
    poll_s: float = Field(default=1, gt=0)

    @property
    def location_env(self) -> str:
        return self.stream_env


class DynamoDbStreamFilter(_Strict):
    attribute: DottedPath
    equals: str


class DynamoDbStreamSourceConfig(EventSourceConfig):
    """The new image of each item a DynamoDB table writes, from its stream. Contract:
    docs/specs/tentacle.md#kinds.
    """

    kind: Literal["dynamodb_stream"]
    # Holds the table name, the same in every region.
    table_env: EnvName
    regions: Regions
    # Keeps only records whose new image holds `equals` at `attribute`.
    filter: DynamoDbStreamFilter | None = None
    poll_s: float = Field(default=1, gt=0)

    @property
    def location_env(self) -> str:
        return self.table_env


class SnsFanoutSourceConfig(EventSourceConfig):
    """The notifications of an SNS topic, from the queue `fire-{system}-{id}` subscribed to it,
    which FIRE never creates. Contract: docs/specs/tentacle.md#kinds.
    """

    kind: Literal["sns_fanout"]
    # Holds the topic name, the same in every region.
    topic_env: EnvName
    regions: Regions
    # The subscription delivers the message itself, not the SNS notification around it.
    raw_delivery: bool = False
    # Long polling: seconds a receive waits for a message.
    wait_s: int = Field(default=5, ge=0, le=20)
    # Seconds a received message stays hidden from the next receive while it is sent; at least
    # 30, so a receive does not hand back messages still in flight.
    visibility_s: int = Field(default=300, ge=30, le=43200)
    # Seconds to wait after a receive that failed or got nothing.
    poll_s: float = Field(default=1, gt=0)

    @property
    def location_env(self) -> str:
        return self.topic_env


class S3CaptureSourceConfig(_Source):
    """Request and response pairs a system's clients write to a bucket. Contract:
    docs/specs/tentacle.md#kinds.
    """

    kind: Literal["s3_capture"]
    bucket_env: EnvName
    # Per client: its key prefix, the label that starts its channel names, and its layout.
    captures: list[CaptureRule] = Field(min_length=1)
    poll_s: float = Field(default=5, gt=0)
    # A response is read once it is this old, so both halves have landed.
    settle_s: float = Field(default=10, ge=0)
    max_object_bytes: int = Field(default=1_048_576, ge=1)
    # A work segment that does not match in full gives no work_id; unset, every segment does.
    work_id_pattern: str | None = Field(default=None, min_length=1)

    @field_validator("work_id_pattern")
    @classmethod
    def _pattern_compiles(cls, pattern: str | None) -> str | None:
        if pattern is not None:
            try:
                re.compile(pattern)
            except re.error as error:
                raise ValueError(f"work_id_pattern is not a regular expression: {error}") from None
        return pattern

    @model_validator(mode="after")
    def _prefixes_do_not_overlap(self) -> S3CaptureSourceConfig:
        prefixes = [capture.prefix for capture in self.captures]
        duplicates = sorted({prefix for prefix in prefixes if prefixes.count(prefix) > 1})
        if duplicates:
            raise ValueError(f"duplicate capture prefixes: {', '.join(duplicates)}")
        nested = sorted(
            {
                inner
                for inner in prefixes
                for outer in prefixes
                if inner != outer and inner.startswith(outer)
            }
        )
        if nested:
            raise ValueError(f"capture prefixes inside another prefix: {', '.join(nested)}")
        return self

    @property
    def location_env(self) -> str:
        return self.bucket_env


class HttpProxySourceConfig(_Source):
    """A reverse proxy in front of an HTTP service. Contract:
    docs/specs/http-and-span-capture.md#inbound-proxy.
    """

    kind: Literal["http_proxy"]
    listen_port: int = Field(ge=1, le=65535)
    listen_host: str = Field(default="127.0.0.1", min_length=1)
    # Holds the upstream's base URL, such as http://localhost:8020.
    upstream_env: EnvName
    openapi_path: str = Field(default="/openapi.json", pattern=r"^/\S*$")
    openapi_refresh_s: float = Field(default=300, gt=0)
    max_body_bytes: int = Field(default=1_048_576, ge=1)
    # Holds the path of a copy of the upstream's OpenAPI document, which the body reads at
    # startup for coverage; the proxy never does. Contract:
    # docs/specs/ingest-drift-and-feedback.md#coverage-and-contract.
    openapi_file_env: EnvName | None = None

    @property
    def location_env(self) -> str:
        return self.upstream_env


class OtlpSourceConfig(_Source):
    """An OTLP/HTTP JSON receiver for the system's traces. Contract:
    docs/specs/http-and-span-capture.md#otlp-receiver.
    """

    kind: Literal["otlp"]
    # Holds the address the receiver listens on, host:port, such as 127.0.0.1:4318.
    listen_env: EnvName
    # The span event a client adds when it writes a capture. Contract:
    # docs/specs/http-and-span-capture.md#capture-events.
    capture_event: CaptureEvent | None = None

    @property
    def location_env(self) -> str:
        return self.listen_env


SourceConfig = Annotated[
    ReplaySourceConfig
    | KinesisSourceConfig
    | DynamoDbStreamSourceConfig
    | SnsFanoutSourceConfig
    | S3CaptureSourceConfig
    | HttpProxySourceConfig
    | OtlpSourceConfig,
    Field(discriminator="kind"),
]


class PolicyConfig(_Strict):
    critical: list[str] = Field(default_factory=list)
    ignore: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _critical_and_ignore_are_disjoint(self) -> PolicyConfig:
        both = sorted(set(self.critical) & set(self.ignore))
        if both:
            raise ValueError(f"names both critical and ignored: {', '.join(both)}")
        return self


class FingerprintingConfig(_Strict):
    sample_rate: float = Field(default=1.0, gt=0, le=1)


class TriggersConfig(_Strict):
    """What opens incidents besides drift, and how triggers correlate; each block is off unless
    declared.
    """

    # 5xx bursts per route. Contract: docs/specs/ingest-drift-and-feedback.md#5xx-bursts.
    http_errors: HttpErrorsTrigger | None = None
    # Which incident.io incidents belong to the system. Contract:
    # docs/specs/ingest-drift-and-feedback.md#incidentio-webhook.
    incident_io: IncidentIoTrigger | None = None
    # Fingerprints whose work units are abandoned. Contract:
    # docs/specs/ingest-drift-and-feedback.md#dlq-attribution.
    dlq: DlqTrigger | None = None
    # How long an incident collects the system's later triggers; absent, each trigger opens its
    # own incident. Contract: docs/specs/ingest-drift-and-feedback.md#correlation.
    correlation: CorrelationPolicy | None = None


class SystemConfig(_Strict):
    system: Slug
    display_name: str
    notify: NotifyConfig
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    sources: list[SourceConfig] = Field(default_factory=list)
    # How outbound calls are named. Contract: docs/specs/http-and-span-capture.md#operation-names.
    operations: OperationRules = Field(default_factory=OperationRules)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    fingerprinting: FingerprintingConfig = Field(default_factory=FingerprintingConfig)
    drift: DriftPolicy = Field(default_factory=DriftPolicy)
    # When flows settle. Contract: docs/specs/flows-and-paths.md#completion.
    paths: PathPolicy = Field(default_factory=PathPolicy)
    triggers: TriggersConfig = Field(default_factory=TriggersConfig)

    @model_validator(mode="after")
    def _source_ids_are_unique(self) -> SystemConfig:
        ids = [source.id for source in self.sources]
        duplicates = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
        if duplicates:
            raise ValueError(f"duplicate source ids: {', '.join(duplicates)}")
        return self

    @model_validator(mode="after")
    def _listen_addresses_are_unique(self) -> SystemConfig:
        addresses = [
            f"{source.listen_host}:{source.listen_port}"
            for source in self.sources
            if isinstance(source, HttpProxySourceConfig)
        ]
        duplicates = sorted({address for address in addresses if addresses.count(address) > 1})
        if duplicates:
            raise ValueError(f"duplicate listen addresses: {', '.join(duplicates)}")
        return self

    @model_validator(mode="after")
    def _dlq_has_outcomes(self) -> SystemConfig:
        if self.triggers.dlq is not None and not self.outcome_sources():
            raise ValueError("triggers.dlq needs a source with outcome: abandoned")
        return self

    def source_ids(self) -> frozenset[str]:
        return frozenset(source.id for source in self.sources)

    def outcome_sources(self) -> frozenset[str]:
        """The ids of the sources with `outcome: abandoned`."""
        return frozenset(
            source.id
            for source in self.sources
            if isinstance(source, EventSourceConfig) and source.outcome == "abandoned"
        )


def load_system_config(path: Path) -> SystemConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    config = SystemConfig.model_validate(raw)
    if config.system != path.stem:
        raise ValueError(f"{path.name}: system {config.system!r} must match the file name")
    return config


def load_system_configs(directory: Path) -> dict[str, SystemConfig]:
    """Every `<system>.yml` in `directory`; files starting with `_` are skipped."""
    if not directory.is_dir():
        return {}
    configs = [
        load_system_config(path)
        for path in sorted(directory.glob("*.yml"))
        if not path.name.startswith("_")
    ]
    return {config.system: config for config in configs}
