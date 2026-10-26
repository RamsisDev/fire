# Local tooling

`dev/` holds what attaches FIRE to a watched system on one machine, records its traffic as
fixtures, and runs the drift demo. Nothing here is particular to a system: each tool reads the
system YAML, and the examples use the template's example system `acme`
(`config/systems/_template.yml`). Test levels are in the [roadmap](../docs/roadmap.md#test-levels):
L1 replays fixtures with no stack, L2 runs against a system's local stack.

| File | What it does |
|------|--------------|
| `otel-collector-config.yml` | An example collector config: the system's pipeline plus one `otlp_http/fire` exporter to the tentacle's receiver |
| `record_otlp.py` | Records what a collector exports to the tentacle, scrubbed, as fixtures in the format of `tests/fixtures/acme/otlp/` |
| `record_captures.py` | Records capture pairs from an S3-compatible bucket, scrubbed, as fixtures in the format of `tests/fixtures/acme/captures/` |
| `corpora/` | L1 drift corpora derived from `tests/fixtures/acme/`, each a `baseline/` and a `drift/` |
| `replay_corpus.py` | Replays a corpus into a running body through the tentacle's own source, with no stack or AWS |

Related: [tentacle](../docs/specs/tentacle.md), [HTTP and span capture](../docs/specs/http-and-span-capture.md),
[flows and paths](../docs/specs/flows-and-paths.md), [ingest and drift](../docs/specs/ingest-drift-and-feedback.md).

FIRE needs Python 3.13+, uv, and `make install`, which also installs moto. Copy `.env.example`
to `.env` and set `FIRE_API_TOKEN`; `FIRE_BODY_URL` defaults to `http://127.0.0.1:8000`.

## Attaching FIRE to a system's local stack

1. **The system YAML.** Copy `config/systems/_template.yml` to `config/systems/<system>.yml`,
   set `system`, and keep the sources the system has. Every resource name, upstream, and listen
   address goes through an env var the file names (`*_env`); nothing in the system's own
   repository changes. For the demo below, `config/systems/acme.yml` is the template unchanged;
   do not commit it.
2. **The body.** `make run` serves on `127.0.0.1:8000` (`FIRE_HOST`, `FIRE_PORT`) and loads every
   file in `config/systems/`. Point the manifest env var at the system's manifest, and each
   proxy's `openapi_file_env` at a copy of its upstream's OpenAPI document, for `/coverage`.
   `PYTHONUNBUFFERED=1` makes the console notifier's lines appear at once in a pipe or file:

   ```bash
   export ACME_MANIFEST_PATH="$PWD/tests/fixtures/acme/manifest/event_chains_graph.json"
   export ACME_API_OPENAPI_PATH="$PWD/tests/fixtures/acme/openapi/acme_api.json"
   PYTHONUNBUFFERED=1 make run 2>&1 | tee /tmp/fire-body.log
   ```

3. **Spans.** Give the system's collector one more `otlphttp` exporter, as in
   `otel-collector-config.yml`, with `encoding: json`, to the address in the `otlp` source's
   `listen_env`. From a container, `host.docker.internal` reaches the host (on Linux, add
   `extra_hosts: ["host.docker.internal:host-gateway"]` and listen on `0.0.0.0`). Keep the
   collector's other exporters: FIRE's is added, never substituted.
4. **Inbound HTTP.** Each `http_proxy` source listens on its `listen_port` in front of the URL in
   its `upstream_env`; send the system's clients, or a test's requests, to the proxy port.
5. **Captures and events.** `s3_capture` and the event kinds read through boto3: point
   `AWS_ENDPOINT_URL` and fake credentials at a local emulator, or use a real AWS profile. An
   `sns_fanout` source reads FIRE's own queue `fire-<system>-<source id>`, subscribed to the
   topic; FIRE never creates the queue or the subscription.
6. **The tentacle.** Set the env vars the system YAML names, then run every source, or one:

   ```bash
   export ACME_OTLP_LISTEN=127.0.0.1:4318
   export ACME_API_UPSTREAM=http://localhost:8080
   export ACME_CAPTURE_BUCKET=acme-captures
   uv run python -m fire.apps.tentacle --system acme 2>&1 | tee /tmp/fire-tentacle.log
   ```

   A source whose env var is unset is skipped with a log line, and each source logs a counters
   line when it stops. The replay source needs no stack at all:

   ```bash
   ACME_REPLAY_PATH=tests/fixtures/acme/test_payloads \
     uv run python -m fire.apps.tentacle --system acme --source work-unit-samples
   H="X-FIRE-Token: $FIRE_API_TOKEN"
   curl -s -H "$H" http://127.0.0.1:8000/v0/systems/acme/streams | jq '.streams[].key'
   ```

   `/streams` then lists five `event:internal:<event type>:payload` channels; `/paths` and
   `/coverage` are read the same way.

## Recording fixtures

Both recorders keep every key and shape and no value; review each file before committing it.
Fixtures committed here belong to the example system `acme`: never commit recordings of a real
system.

`record_otlp.py` listens where the tentacle's receiver would (stop the tentacle first) and
writes each export to `<out dir>/NNNN.json`. Only the receiver's allowlisted attributes keep
their values; every other value becomes a placeholder of its type, and URLs lose their query
values and userinfo. Run it, send one request through the system, wait for its async work,
then press Ctrl-C:

```bash
uv run python dev/record_otlp.py /tmp/checkout_1 --listen 127.0.0.1:4318
```

`record_captures.py` reads the pairs under each capture prefix of the system's `s3_capture`
sources (or the `--prefix` given, repeatable) and writes them to `<out dir>/<label>/NNN.json`.
Body leaves become placeholders of the same value type, and a pair whose shape would change is
refused; header values, URLs, and error texts become `redacted`, work segments fake ULIDs, and
entity ids `ENTITY<n>`. It exits `1` when it refused a pair.

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  uv run python dev/record_captures.py /tmp/captures --bucket acme-captures \
    --endpoint http://localhost:4566 --prefix billing-payloads/ --limit 3
```

## Drift demo

The drift demo is L1: `replay_corpus.py` replays a corpus of `corpora/` into a running body,
started freshly as above with `config/systems/acme.yml`. It runs one tentacle source of the
system inside the script, as the tentacle runs it, sending to `FIRE_BODY_URL` with
`FIRE_API_TOKEN`: `drift.min_samples` copies (20) of the corpus's `baseline/` items, in turn,
then `--drift-copies` copies (default 1) of its `drift/` items, each copy with fresh ids, so
replaying again adds observations instead of duplicates.

```text
replay_corpus.py outbound|path [--system acme] [--config config/fire.yml] [--drift-copies N]
```

| Corpus | Holds | Replayed through |
|--------|-------|------------------|
| `outbound/` | Shipping `create` pairs. `baseline/001.json` is `tests/fixtures/acme/captures/shipping/001.json`; `002` drops the response's `MessageText`, `003` also `CustomerReference`, both always null. `drift/001.json` is `001` whose response lost `ShipmentNumber`, which every baseline response carries | `acme-captures` (`s3_capture`), on a bucket of moto's in-process S3 that the script fills, one fake work unit per copy; the source waits its `settle_s` (10 s) |
| `path/` | `baseline/checkout_1/` is the recording `tests/fixtures/acme/otlp/checkout_1`; `drift/checkout_1/` is the same without the billing publisher's call `POST billing.partner.example.com/v2/invoices/place` in the worker's trace | `acme-traces` (`otlp`), on a free local port, to which the script posts each request as a collector does; it then polls `/paths` until the flows settle (`paths.settle_s`, 60 s) |

The script drops `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, `AWS_PROFILE`, and
`AWS_SESSION_TOKEN` from its own environment and points the AWS config files at `/dev/null`, so
a shell set up for an emulator or AWS changes nothing.

```bash
uv run python dev/replay_corpus.py outbound
grep -c '^\[fire\] notify' /tmp/fire-body.log   # 1
```

```text
… tentacle counters: source=acme-captures read=21 built=42 sent=42 unsent=0 pending=0 rejected=- skipped=- noted=unjoined:21 body_duplicate=0 body_rejected=-
http:outbound:shipping:create:request {"status": "warm", "samples": 21, "drift": []}
http:outbound:shipping:create:response:2xx {"status": "warm", "samples": 20, "drift": [{"diff": {"added": [], "type_changed": [], "removed": [{"path": "ShipmentNumber", "value_type": "string"}]}, "occurrences": 1}]}
```

`noted=unjoined:21`: no capture event names these pairs, so their shapes carry their work unit
alone. The body prints one notification; with `ACME_MANIFEST_PATH` set, the last possibility
names `process_path` alone:

```text
[fire] notify incident=<uuid> system=acme trigger=shape_drift
*FIRE ENGINE* incident `<uuid>` (`acme`)
Confidence: 20% — `noul` (do not treat as settled cause)
Possibilities:
• (50%) Shape drift on http:outbound:shipping:create:response:2xx: ShipmentNumber (string) removed _[shape_drift]_
• (50%) Insufficient evidence (MISSING fragments) _[coverage, process_path]_
Shape hash: `<schema_hash>`
_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._
```

Run it again with `--drift-copies 99`: 20 more baseline pairs fold, the response stream shows
`"samples": 40` and the signal `"occurrences": 100`, and the body prints no second notification.
The path corpus, on a freshly started body, returns once the flows settle, about a minute later:

```bash
uv run python dev/replay_corpus.py path
```

```text
… tentacle counters: source=acme-traces read=881 built=881 sent=881 unsent=0 pending=0 rejected=- skipped=- noted=capture_indexed:84 body_duplicate=0 body_rejected=-
… replay_corpus waiting for the flows to settle: settle_s=60.0
acme-api/POST /v1/orders/checkout {"status": "warm", "flows": 20, "drifted_flows": 1, "drift": [{"rule": "hop_missing", "node": "POST billing.partner.example.com/v2/invoices/place", "occurrences": 1}]}
```

Each copy is 42 spans, the drift copy 41, with two capture events of two keys each. The body's
completion loop prints one notification: one possibility per hop of the drifted flow's 28, then
the drift (shortened here):

```text
[fire] notify incident=<uuid> system=acme trigger=path_drift
*FIRE ENGINE* incident `<uuid>` (`acme`)
Confidence: 45%
Possibilities:
• (3%) Failure near acme-api/POST /v1/orders/checkout _[process_path:acme-api/POST /v1/orders/checkout]_
…  one line per hop, 28 in all
• (3%) hop_missing: POST billing.partner.example.com/v2/invoices/place on POST /v1/orders/checkout _[path_drift]_
Process path: `acme-api/POST /v1/orders/checkout → … → acme-billing-publisher/DynamoDB.UpdateItem acme-outbox-dev`
_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._
```

Again with `--drift-copies 99`: the path shows `"flows": 40`, `"drifted_flows": 100`, and the
signal `"occurrences": 100`, with no second notification.
`tests/integration/test_drift_demo.py` runs both replays, twice each, into a body in-process.

Inbound drift needs the system's API behind its proxy: warm the request stream past
`drift.min_samples` with valid bodies, varying only optional fields, then send one body with a
required field removed or a type changed. The body notifies once for the request channel, and
the same body again adds an occurrence, not a notification.

## Troubleshooting

- **The collector cannot reach the receiver.** Its logs show `otlp_http/fire` export errors and
  the tentacle never logs `spans received`. On Linux, listen on `0.0.0.0:4318`, not
  `127.0.0.1:4318`. `request refused: … status=415` means the exporter sends protobuf: set
  `encoding: json`.
- **A tentacle port is busy.** The tentacle exits `2` with `cannot listen on <address>`;
  `lsof -nP -iTCP:<port> -sTCP:LISTEN` names the process. On macOS, `launchd` can hold `:8021`.
- **No inbound streams.** `skipped=templates_unavailable:<n>` while the proxy cannot fetch its
  upstream's OpenAPI document, `unmatched_route:<n>` for paths it does not declare; a `502` with
  `x-fire-proxy: upstream-unreachable` means the upstream is not answering.
- **No capture or event streams.** An unset `*_env` skips the source; an AWS error is logged once
  per source and the source keeps polling. `noted=unjoined:<n>` with no `capture_indexed` on the
  `otlp` source means its `capture_event` is missing or the receiver is not running: pairs still
  arrive, joined by work unit alone.
- **401 from FIRE.** The body and the caller hold different `FIRE_API_TOKEN` values; an exported
  variable wins over `.env`. A `503` means the body has no token.
- **`tentacle not started: … No such file or directory: …/systems/acme.yml`.** The replay and the body read
  `config/systems/<system>.yml`; copy the template there first, or pass `--system` and
  `--config`.
