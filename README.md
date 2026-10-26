![FIRE ENGINE — Failure Investigation & Root Explanation](docs/assets/cover.jpg)

AI-assisted incident response that reasons over **payload shape** (never raw field
values) and the **process path** a request walked — which services, spans, and
changes it hit — then ranks **why it may have failed**.

Ingest → correlate → assemble a dossier (shape + path + context) → System One
reasoner → **Narrator** (LLM prose) → notifier. Target: calibrated possibilities
in under 30 seconds, then a readable Slack message.

## Purpose

Existing IR tools are weak at connecting “what the payload *looked like*” with
“which path through the system it took.” FIRE ENGINE is built for that gap:

1. **Ingest** structured trace/object streams from watched transaction systems,
   storing **fingerprints / shapes**, not secret values.
2. **Assemble** an incident dossier: shape drift, process path (span/service
   chain), change timeline, dependency health, visibility gaps, runbooks.
3. **Reason** with a **System One** model over that dossier only — shape + path →
   **dynamic calibrated possibilities**, never raw PII/payload bodies and never a
   free-form LLM essay as the decision path.
4. **Narrate** — an LLM **Narrator** turns the decision + shape/path fragments into
   a coherent Slack message (prose only; cannot invent cause or see raw values).
5. **Notify** the on-call channel with that message.

FIRE knows no watched system. Onboarding one means writing a YAML file
(`config/systems/<system>.yml`) from the template; every source kind, manifest format, and
trigger is a generic port that the YAML configures.

## Architecture

Hexagonal, with a strict dependency rule:

- `fire/core/` — domain, ports, and services. **Zero knowledge of concrete
  watched systems and zero infra deps** (only `pydantic` and the standard library).
- `fire/adapters/` — concrete port implementations (sources, manifests, shapes,
  catalogs, reasoners, narrators, notifiers, webhooks). Imports `core`.
- `fire/apps/` — runnable apps: the incident engine (the *body*) and the tentacle.
  Imports both.
- `config/systems/` — one YAML per watched system; all per-system variability
  lives here, never in code. It declares decisions (Slack channel id, manifest
  location, sources, policy exceptions, triggers); what the system contains is discovered
  from its manifest, never listed by hand (`docs/specs/system-config-and-discovery.md`).
  The repo ships only `_template.yml`.

```
apps  --imports-->  adapters  --imports-->  core        (never the other way)
```

### Body and tentacle

The **tentacle** (`python -m fire.apps.tentacle --system <system>`) runs next to a watched
system. It reads the sources the system's YAML declares, turns each payload into a shape and
a fingerprint, and sends only those, with attribute-free spans, to the body. Raw values never
leave it (`docs/specs/tentacle.md`). Source kinds:

| Kind | Reads |
|------|-------|
| `replay` | `.json` / `.jsonl` files or a directory, once |
| `kinesis` | A Kinesis data stream, per region |
| `dynamodb_stream` | A DynamoDB table's stream, per region (also outcome sources such as an outbox of abandoned work) |
| `sns_fanout` | An SNS topic, through a FIRE-owned SQS queue subscribed to it |
| `http_proxy` | Inbound HTTP, as a reverse proxy in front of a service, with route templates from its OpenAPI document |
| `otlp` | OTLP/HTTP JSON spans from an OpenTelemetry collector |
| `s3_capture` | Request and response pairs a system's clients write to a bucket, joined to their spans |

The **body** (`python -m fire.apps.incident_engine`) stores observations, learns baselines and
paths, raises triggers (shape drift, path drift, 5xx bursts, DLQ attribution, incident.io
webhooks), correlates them into incidents, and diagnoses each one.

### Pipeline (target)

Alert / ingest returns **202 at T=0**; diagnosis runs in background:

1. **Correlate** — collapse a system's triggers inside a window into one incident.
2. **Assemble dossier** — collectors fan out concurrently with per-tier budgets;
   missing evidence is declared `MISSING`, never invented.
3. **Reason** — System One (`choice` / `score` / `noul`) over shape + process
   path (+ changes, deps, runbooks) → calibrated possibility distribution.
4. **Narrate** — LLM Narrator formats decision + safe fragments → Slack prose.
5. **Notify** — console locally; Slack when configured.

| Tier | Meaning | Default budget |
|------|---------|----------------|
| `TIER_0` | Internal reads (baseline, change ledger, visibility, deps) | ~300 ms |
| `TIER_1` | External pulls (vendor APIs / deep window capture) | ~3000 ms |
| `TIER_2` | Bonus context (runbook search / RAG) | ~1500 ms |

### Reasoner contract (non-negotiable)

- Input: **shape-only** payload evidence + **process path** (services/spans/changes).
  No raw field values.
- Engine: **System One**, a fast calibrated model, not a full generative LLM on the happy path.
  - Primitives: `choice` / `score` / `noul` over the dossier.
  - Output: **dynamic calibrated possibilities** (distribution + confidence), not
    prose root-cause. Option sets for `choice` can be built per incident from the
    path/hypotheses.
  - `noul` is the privacy gate when evidence is insufficient or unsafe to score.
- Cascade: System One first → low confidence → human and/or optional generative LLM
  escalation. Generative LLM is **not** the default reasoner.
- Today the `echo` stub stands behind the port; a System One reasoner adapter over HTTP
  (`FIRE_SYSTEMONE_BASE_URL`) comes next.

### Narrator contract (non-negotiable)

The reasoner **decides**; the **Narrator** only writes readable prose for Slack.

- Input: System One decision (possibilities + confidence) + shape/path fragments
  already in the dossier. **No raw payload values.**
- Output: coherent channel message. Must **not** invent a root cause, raise
  confidence, or introduce facts absent from the decision/dossier.
- Default adapter: **OpenAI-compatible** chat Completions via env
  (`FIRE_NARRATOR_BASE_URL`, `FIRE_NARRATOR_API_KEY`, `FIRE_NARRATOR_MODEL`).
  Keys never live in the repo.
- A local template narrator stands in for demos behind the same port.

## Current state

v0 is in tree: hexagonal `fire/{core,adapters,apps}`, ports Reasoner ≠ Narrator ≠ Notifier,
echo, template, console, and Slack adapters, and the FastAPI body with fail-closed auth.
The engine validates `config/systems/*.yml` at startup, discovers each system's inventory from
its manifest (`event_chains_graph` format) for the coverage audit, and recomputes the
fingerprint of every shape it receives. The tentacle runs every source kind above.
The body assembles spans into **flows** that cross async hops through span links, learns one
**path** per entry operation, and opens a `path_drift` incident, with the flow's process path,
when a complete flow adds, misses, or fails a hop (`docs/specs/flows-and-paths.md`). Shape
drift, 5xx bursts, DLQ attribution, and incident.io webhooks open incidents too, correlated
per system inside a window (`docs/specs/ingest-drift-and-feedback.md`).

Next: the System One reasoner adapter, the OpenAI-compatible Narrator, and the feedback loop
(`docs/roadmap.md`). No published accuracy claims: no benchmark numbers in docs until measured.

## Development

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
make install            # uv sync --group dev
make fmt                # ruff format
make lint               # ruff format --check + ruff check
make test               # pytest
make run                # local engine on :8000
```

Copy `.env.example` to `.env` and set `FIRE_API_TOKEN`. CI runs the same format check, lint,
and tests on every pull request. `dev/` holds local tooling for any watched system: recording
OTLP exports and capture pairs as scrubbed fixtures, and replaying drift corpora into a running
body (`dev/README.md`).

### Onboarding a system

```bash
cp config/systems/_template.yml config/systems/acme.yml   # set system: acme, then decide
make run                                                  # the body loads and validates it
uv run python -m fire.apps.tentacle --system acme --source work-unit-samples
```

Every env var the YAML names (`*_env`) goes into `.env`; an unset one makes the tentacle skip
that source and say so. Tests use the synthetic example system `acme` and its fixtures under
`tests/fixtures/acme/`.

## HTTP API

Auth is **required by default** on mutating endpoints (`X-FIRE-Token` shared secret).
Missing/invalid token → `401`; server token env var (`FIRE_API_TOKEN`) unset → `503`.
No fail-open “secret optional” mode.

- `GET /health` — liveness (unauthenticated).
- `POST /v0/diagnose` — submit a `manual` incident for a loaded `system`; the fingerprint of
  a `shape` is recomputed and a different client one is `422`; `202` with an `incident_id`,
  diagnosis runs in the background (`docs/specs/dossier-and-decision.md`).
- `POST /v0/observations` — ingest shape and span observations; `202` with accepted,
  duplicate, and rejected counts per list (`docs/specs/observations.md`). New shape drift
  signals, 5xx bursts, and DLQ attributions become triggers, correlated into incidents and
  diagnosed in the background (`docs/specs/ingest-drift-and-feedback.md`).
- `GET /v0/systems/{system}/streams` — baseline status and samples, observation and
  fingerprint counts, and open shape drift signals, per stream key.
- `GET /v0/systems/{system}/paths` — completes settled flows, then lists open flows, flow
  counters, and one path per entry operation with its nodes, edges, and path drift signals
  (`docs/specs/flows-and-paths.md`).
- `GET /v0/systems/{system}/coverage` — the manifest's events never observed, each inbound
  proxy's OpenAPI routes never called, and the outbound operations observed, or why a part has
  nothing to compare.
- `GET /v0/systems/{system}/dlq` — abandoned work units joined to the fingerprints that
  carried them.
- `POST /v0/webhooks/incident-io` — incident.io webhooks, verified by signature instead of the
  token; `503` while the signing secret is unset.

## Contributing

Read `docs/specs/` before changing a module: each spec is the contract of the modules it names,
and a behavior change updates its spec in the same change. Agent and contributor conventions
live in `.cursor/rules/`. Nothing particular to a real watched system is committed here: no
system YAML of a real system, and no recordings or fixtures taken from one.
