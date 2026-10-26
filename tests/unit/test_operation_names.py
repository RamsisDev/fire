"""Operation names: the naming rules, the id rules, per-host templates, and their config."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from canary import Canary
from pydantic import ValidationError

from fire.adapters.sources.otlp import parse_traces
from fire.core.domain.observation import SpanKind
from fire.core.domain.operation import (
    ID_RULES,
    MAX_OPERATION_LENGTH,
    OperationInputs,
    OperationName,
    OperationNamer,
    OperationRules,
    generalize_segment,
    generalize_span_name,
    http_method,
    is_id,
)

PARTNER = {
    "hosts": ["*.partner.example.com"],
    "alias": "partner",
    "paths": ["/api/{version}/orders", "/api/{version}/orders/{order_id}"],
}
NAMER = OperationNamer(OperationRules.model_validate({"http": [PARTNER]}))


def _server(method: str | None = "POST", route: str | None = None, **kw: Any) -> OperationInputs:
    return OperationInputs(SpanKind.SERVER, kw.pop("span_name", "server"), method, route, **kw)


def _client(url: str | None, method: str | None = "GET", **kw: Any) -> OperationInputs:
    return OperationInputs(SpanKind.CLIENT, kw.pop("span_name", "GET"), method, url=url, **kw)


def _aws(
    service: str, method: str, kind: SpanKind = SpanKind.CLIENT, **kw: Any
) -> OperationInputs:
    return OperationInputs(
        kind,
        f"{service}.{method}",
        rpc_system="aws-api",
        rpc_service=service,
        rpc_method=method,
        **kw,
    )


def _internal(span_name: str) -> OperationInputs:
    return OperationInputs(SpanKind.INTERNAL, span_name)


def _name(span: OperationInputs, namer: OperationNamer = NAMER) -> str | None:
    named = namer.name(span)
    return None if named is None else named.name


# The examples of docs/specs/http-and-span-capture.md#examples.
EXAMPLES = {
    "server-route": (_server(route="/v1/checkout"), "POST /v1/checkout"),
    "server-unrouted": (_server("GET"), "GET {unrouted}"),
    "declared-host": (
        _client("https://api.partner.example.com/api/v2/orders/12345?email=a@b.c"),
        "GET partner/api/{version}/orders/{order_id}",
    ),
    "declared-unlisted": (
        _client("https://api.partner.example.com/api/v2/refunds"),
        "GET partner/{unlisted}",
    ),
    "undeclared-host": (
        _client("https://other.example.net/v1/customers/SO-1234567/items"),
        "GET other.example.net/v1/customers/{id}/items",
    ),
    "ip-host": (
        _client("http://10.0.3.7:9000/hooks/0f8fad5b-d9cb-469f-a165-70867728950e", "POST"),
        "POST {ip}:9000/hooks/{id}",
    ),
    "dynamodb": (
        _aws("DynamoDB", "PutItem", dynamodb_tables=("acme-dev-orders",)),
        "DynamoDB.PutItem acme-dev-orders",
    ),
    "sns": (
        _aws(
            "SNS",
            "Publish",
            SpanKind.PRODUCER,
            sns_topic_arn="arn:aws:sns:us-east-1:123456789012:acme-dev-order-events",
        ),
        "SNS.Publish acme-dev-order-events",
    ),
    "s3-no-bucket": (_aws("S3", "PutObject"), "S3.PutObject"),
    "asgi-send": (
        _internal("POST /v1/checkout http send"),
        "POST /v1/checkout http send",
    ),
    "span-name-ulid": (_internal("retry 01J9ZKX4W3V8Q2T6M5N7P0R1S2"), "retry {id}"),
}


@pytest.mark.unit
@pytest.mark.parametrize(("span", "expected"), list(EXAMPLES.values()), ids=list(EXAMPLES))
def test_spec_examples(span: OperationInputs, expected: str) -> None:
    assert _name(span) == expected


# Per rule: values only it catches, and near misses that every rule lets pass.
ID_RULE_CASES: dict[str, tuple[list[str], list[str]]] = {
    "digits": (["42", "0", "300000123456789"], ["4a", "", "٤٢"]),
    "uuid": (
        [
            "0f8fad5b-d9cb-469f-a165-70867728950e",
            "0F8FAD5B-D9CB-469F-A165-70867728950E",
            "abcdefab-abcd-abcd-abcd-abcdefabcdef",
        ],
        ["abcdefab-abcd-abcd-abcd-abcdefabcde", "abcdefababcd-abcd-abcd-abcdefabcdef"],
    ),
    "ulid": (
        ["01J9ZKX4W3V8Q2T6M5N7P0R1S2", "7zzzzzzzzzzzzzzzzzzzzzzzzz"],
        ["8ZZZZZZZZZZZZZZZZZZZZZZZZZ", "01J9ZKX4W3V8Q2T6M5N7P0R1S", "01I9ZKX4W3V8Q2T6M5N7P0R1S2"],
    ),
    "hex": (["5f2b9c0d1e3a4b6c", "deadbeefdeadbeef"], ["abcdefabcdefabc", "deadbeef"]),
    "mixed": (["SO-1234567", "11.13.18.05", "order123"], ["order12", "v1", "orderitems"]),
    "unsafe": (["jane@example.com", "x=1", "a b", "é", "a:b", "a/b"], ["a-b.c_d~e"]),
    "long": (["a" * 65], ["a" * 64]),
}


@pytest.mark.unit
@pytest.mark.parametrize("rule", sorted(ID_RULES))
def test_each_id_rule_alone(rule: str) -> None:
    caught, near_misses = ID_RULE_CASES[rule]
    for segment in caught:
        assert ID_RULES[rule](segment), segment
        assert is_id(segment), segment
    for segment in near_misses:
        assert not ID_RULES[rule](segment), segment


@pytest.mark.unit
def test_every_rule_has_cases() -> None:
    assert set(ID_RULE_CASES) == set(ID_RULES)


@pytest.mark.unit
@pytest.mark.parametrize(
    "segment",
    ["v1", "api-v2", "oauth2", "deadbeef", "order-items", "cancel_order", "abcdefabcdefabc", ""],
)
def test_names_pass_every_id_rule(segment: str) -> None:
    assert not is_id(segment)
    assert generalize_segment(segment) == segment


@pytest.mark.unit
@pytest.mark.parametrize(
    ("segment", "expected"),
    [
        ("%7Eorders", "~orders"),
        ("ord%65rs", "orders"),
        ("a%2Fb", "{id}"),
        ("%zz", "{id}"),
        ("%E2%9C%93", "{id}"),
        ("{order_id}", "{order_id}"),
        ("{order-id.v2}", "{order-id.v2}"),
        ("%7Border_id%7D", "{id}"),
    ],
)
def test_segments_are_percent_decoded_and_placeholders_kept(segment: str, expected: str) -> None:
    assert generalize_segment(segment) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("billing-publisher.process_work_unit", "billing-publisher.process_work_unit"),
        ("GET /v1/orders/{order_id} http send", "GET /v1/orders/{order_id} http send"),
        ("acme-orders send", "acme-orders send"),
        ("fetch /v1/orders/300000123456789", "fetch /v1/orders/{id}"),
        ("lookup jane@example.com now", "lookup {id} now"),
        ("a  b//c", "a  b//c"),
    ],
)
def test_span_names_go_through_the_id_rules(name: str, expected: str) -> None:
    assert generalize_span_name(name) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "expected"),
    [
        ("get", "GET"),
        ("Patch", "PATCH"),
        ("QUERY", "QUERY"),
        ("CONNECT", "CONNECT"),
        ("_OTHER", "HTTP"),
        ("PROPFIND", "HTTP"),
    ],
)
def test_methods_are_uppercase_or_http(method: str, expected: str) -> None:
    assert http_method(method) == expected
    assert _name(_server(method, "/x")) == f"{expected} /x"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("url", "kw", "expected"),
    [
        ("https://API.Example.NET/v1", {}, "GET api.example.net/v1"),
        ("http://example.net:80/v1", {}, "GET example.net/v1"),
        ("https://example.net:443/v1", {}, "GET example.net/v1"),
        ("http://example.net:8080/v1", {}, "GET example.net:8080/v1"),
        ("https://user:secret@example.net/v1", {}, "GET example.net/v1"),
        ("https://example.net", {}, "GET example.net/"),
        ("https://example.net?q=1", {}, "GET example.net/"),
        ("https://example.net//v1/", {}, "GET example.net//v1/"),
        ("http://[2001:db8::1]:9000/x", {}, "GET {ip}:9000/x"),
        ("https://example.net:99999/v1", {}, "GET example.net/v1"),
        (
            "https://example.net/v1",
            {"server_address": "Gateway.Example.NET"},
            "GET gateway.example.net/v1",
        ),
        ("https://example.net/v1", {"server_port": 8443}, "GET example.net:8443/v1"),
        (None, {"server_address": "example.net", "server_port": 9000}, "GET example.net:9000"),
        ("https://exa mple.net/v1", {}, "GET"),
    ],
    ids=[
        "lowercased",
        "port-80",
        "port-443",
        "port-kept",
        "userinfo-dropped",
        "no-path",
        "query-dropped",
        "empty-segments-kept",
        "ipv6",
        "bad-port",
        "server-address-wins",
        "server-port-wins",
        "no-url",
        "bad-host-falls-to-span-name",
    ],
)
def test_client_hosts_ports_and_paths(url: str | None, kw: dict[str, Any], expected: str) -> None:
    assert _name(_client(url, **kw)) == expected


@pytest.mark.unit
def test_rules_apply_in_order() -> None:
    both = _server(route="/v1/x", rpc_system="aws-api", rpc_service="SQS", rpc_method="Send")
    assert _name(both) == "SQS.Send"
    assert _name(_server(None, "/v1/x", span_name="GET /v1/x")) == "GET /v1/x"
    assert _name(_client(None, span_name="POST")) == "POST"
    assert _name(_client("https://example.net/v1", None, span_name="send it")) == "send it"
    producer = OperationInputs(SpanKind.PRODUCER, "orders publish", "POST", url="https://x.net/")
    assert _name(producer) == "orders publish"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("span", "expected"),
    [
        (
            _aws("DynamoDB", "BatchGetItem", dynamodb_tables=("b-table", "a-table")),
            "DynamoDB.BatchGetItem a-table,b-table",
        ),
        (_aws("DynamoDB", "TransactWriteItems"), "DynamoDB.TransactWriteItems"),
        (_aws("SNS", "Publish", sns_topic_arn="topic-only"), "SNS.Publish topic-only"),
        (
            _aws(
                "SNS",
                "Publish",
                messaging_destination_name="arn:aws:sns:us-east-1:123456789012:t",
                messaging_destination="arn:aws:sns:us-east-1:123456789012:t",
            ),
            "SNS.Publish",
        ),
        (
            _aws("SQS", "SendMessage", messaging_destination="orders-queue"),
            "SQS.SendMessage orders-queue",
        ),
        (
            _aws(
                "SQS", "ReceiveMessage", messaging_destination_name="a", messaging_destination="b"
            ),
            "SQS.ReceiveMessage a",
        ),
        (_aws("S3", "GetObject", s3_bucket="captures"), "S3.GetObject captures"),
        (_aws("Lambda", "Invoke", s3_bucket="ignored"), "Lambda.Invoke"),
        (
            OperationInputs(
                SpanKind.CLIENT,
                "x",
                rpc_system="grpc",
                rpc_service="orders.Orders",
                rpc_method="Get",
            ),
            "orders.Orders/Get",
        ),
        (
            OperationInputs(
                SpanKind.CLIENT, "DynamoDB.PutItem", rpc_system="aws-api", rpc_service="DynamoDB"
            ),
            "DynamoDB.PutItem",
        ),
    ],
    ids=[
        "dynamodb-batch-sorted",
        "dynamodb-no-table",
        "sns-no-colon",
        "sns-never-reads-destinations",
        "sqs-destination",
        "sqs-destination-name-first",
        "s3-bucket",
        "other-service-no-resource",
        "other-rpc-system",
        "rpc-without-method-falls-to-span-name",
    ],
)
def test_aws_and_rpc_names(span: OperationInputs, expected: str) -> None:
    assert _name(span) == expected


@pytest.mark.unit
def test_template_specificity_and_unlisted() -> None:
    rule = {
        "hosts": ["api.partner.example.com", "*.partner.test"],
        "alias": "partner",
        "paths": [
            "/api/{version}/orders/{order_id}",
            "/api/v2/orders/{order_id}",
            "/api/{version}/orders/summary",
            "/",
        ],
    }
    namer = OperationNamer(OperationRules.model_validate({"http": [rule]}))
    host = "https://api.partner.example.com"
    cases = {
        f"{host}/api/v2/orders/7": "partner/api/v2/orders/{order_id}",
        f"{host}/api/v3/orders/7": "partner/api/{version}/orders/{order_id}",
        f"{host}/api/v3/orders/summary": "partner/api/{version}/orders/summary",
        f"{host}:8443/": "partner/",
        "https://eu.partner.test/api/v2/orders/%37": "partner/api/v2/orders/{order_id}",
        f"{host}/api/v2/orders/": "partner/{unlisted}",
        "https://partner.test/api/v2/orders/7": "partner.test/api/v2/orders/{id}",
    }
    for url, expected in cases.items():
        assert _name(_client(url), namer) == f"GET {expected}", url
    assert namer.name(_client("https://api.partner.example.com/api/v2/orders/")) == OperationName(
        "GET partner/{unlisted}", unlisted=True
    )
    assert namer.name(_client(None, server_address="api.partner.example.com")) == OperationName(
        "GET partner"
    )


@pytest.mark.unit
def test_first_declared_host_wins_and_a_host_without_paths_is_closed() -> None:
    rules = {
        "http": [
            {"hosts": ["*.example.com"], "alias": "closed"},
            {"hosts": ["api.example.com"], "alias": "never", "paths": ["/v1"]},
        ]
    }
    namer = OperationNamer(OperationRules.model_validate(rules))
    assert _name(_client("https://api.example.com/v1"), namer) == "GET closed/{unlisted}"


@pytest.mark.unit
def test_names_longer_than_256_characters_or_empty_are_none() -> None:
    fits = "/abc" * 61 + "/ab"
    assert len(f"GET x.net{fits}") == MAX_OPERATION_LENGTH
    assert _name(_client(f"https://x.net{fits}")) == f"GET x.net{fits}"
    assert _name(_client(f"https://x.net{fits}c")) is None
    assert NAMER.name(_internal("")) is None
    assert NAMER.name(_internal("   ")) is None


@pytest.mark.unit
def test_operation_inputs_repr_hides_the_url_and_destinations() -> None:
    span = _client(
        "https://x.net/v1?email=jane@example.com",
        sns_topic_arn="arn:aws:sns:us-east-1:123456789012:t",
        messaging_destination="https://sqs.us-east-1.amazonaws.com/123456789012/q",
    )
    assert "jane" not in repr(span)
    assert "123456789012" not in repr(span)


@pytest.mark.unit
def test_planted_values_in_path_positions_never_appear(canary: Canary) -> None:
    for _ in range(30):
        value = canary.string()
        token = value.rsplit("/", 1)[-1].split("@")[0]
        named = [
            _name(_client(f"https://x.net/v1/{value}?q={canary.token()}")),
            _name(_client(f"https://x.net/v1/{token}/items")),
            _name(_client(f"https://api.partner.example.com/api/{token}/orders/{canary.token()}")),
            _name(_internal(f"retry {token} /v1/{canary.number()}")),
        ]
        assert canary.leaks(" ".join(str(name) for name in named)) == []


RECORDINGS = Path(__file__).resolve().parents[1] / "fixtures" / "acme" / "otlp"
BILLING = "billing.partner.example.com"
PAYMENTS = "payments.partner.example.com"
# Every operation of one recorded checkout, from tests/fixtures/acme/otlp/.
CHECKOUT_OPERATIONS = {
    ("server", "POST /v1/orders/checkout"),
    ("internal", "POST /v1/orders/checkout http receive"),
    ("internal", "POST /v1/orders/checkout http send"),
    ("internal", "tax.lookup_code"),
    ("internal", "tax.estimate"),
    ("internal", "worker.distribute_work"),
    ("internal", "billing-publisher.process_work_unit"),
    ("internal", "billing-publisher.idempotency_check"),
    ("internal", "billing-publisher.business_logic"),
    ("internal", "billing-publisher.outbox_recording"),
    ("producer", "SNS.Publish acme-billing-events-dev"),
    ("client", "DynamoDB.PutItem acme-dev-orders"),
    ("client", "DynamoDB.BatchGetItem acme-catalog-products"),
    ("client", "DynamoDB.GetItem acme-catalog-products"),
    ("client", "DynamoDB.UpdateItem acme-dev-worker_leases"),
    ("client", "DynamoDB.TransactWriteItems"),
    ("client", "DynamoDB.PutItem acme-outbox-dev"),
    ("client", "DynamoDB.GetItem acme-outbox-dev"),
    ("client", "DynamoDB.UpdateItem acme-outbox-dev"),
    ("client", "DynamoDB.Query acme-dev-billing_orders"),
    ("client", "S3.PutObject"),
    ("client", "GET identity.example.com/api/v1/users"),
    ("client", f"POST {PAYMENTS}/v1/payments/order-payments/create"),
    ("client", f"POST {PAYMENTS}/v2/tax/calculate-tax/estimate"),
    ("client", f"POST {PAYMENTS}/v2/tax/tax-code"),
    ("client", f"POST {BILLING}/v2/invoices/place"),
}


def _recorded_operations(name: str) -> set[tuple[str, str]]:
    names = set()
    for path in sorted((RECORDINGS / name).glob("*.json")):
        for raw in parse_traces(json.loads(path.read_text(encoding="utf-8"))):
            named = OperationNamer().name(raw.naming)
            assert named is not None
            names.add((raw.kind.value, named.name))
    return names


@pytest.mark.unit
def test_two_recordings_of_one_flow_give_the_same_operation_names() -> None:
    first = _recorded_operations("checkout_1")
    second = _recorded_operations("checkout_2")
    assert first == second == CHECKOUT_OPERATIONS
    assert len(CHECKOUT_OPERATIONS) == 26


VALID_RULE = {"hosts": ["api.example.com"], "alias": "api", "paths": ["/v1/{order_id}"]}


def _rule_error_locs(**overrides: Any) -> list[tuple[str | int, ...]]:
    with pytest.raises(ValidationError) as caught:
        OperationRules.model_validate({"http": [{**VALID_RULE, **overrides}]})
    return [error["loc"] for error in caught.value.errors()]


@pytest.mark.unit
def test_operations_defaults_to_no_declared_hosts() -> None:
    assert OperationRules().http == []
    assert OperationRules.model_validate({"http": [VALID_RULE]}).http[0].alias == "api"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("overrides", "loc"),
    [
        ({"alias": "Partner"}, ("http", 0, "alias")),
        ({"alias": "part_ner"}, ("http", 0, "alias")),
        ({"alias": "a" * 65}, ("http", 0, "alias")),
        ({"hosts": []}, ("http", 0, "hosts")),
        ({"hosts": ["API.example.com"]}, ("http", 0, "hosts", 0)),
        ({"hosts": ["*example.com"]}, ("http", 0, "hosts", 0)),
        ({"hosts": ["api.*.com"]}, ("http", 0, "hosts", 0)),
        ({"paths": ["v1/orders"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/{Order}"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/{order-id}"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/order{id}"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/a b"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/a@b"]}, ("http", 0, "paths")),
        ({"paths": ["/v1/{a}", "/v1/{b}"]}, ("http", 0, "paths")),
        ({"paths": ["/v1", "/v1"]}, ("http", 0, "paths")),
        ({"paths": ["/" + "a" * 240]}, ("http", 0, "paths")),
        ({"alias": "a" * 64, "paths": ["/" + "a" * 184]}, ("http", 0)),
        ({"methods": ["GET"]}, ("http", 0, "methods")),
    ],
    ids=[
        "alias-uppercase",
        "alias-underscore",
        "alias-long",
        "no-hosts",
        "host-uppercase",
        "host-star-without-dot",
        "host-star-inside",
        "template-relative",
        "placeholder-uppercase",
        "placeholder-dash",
        "placeholder-partial",
        "literal-space",
        "literal-unsafe",
        "duplicate-by-literals",
        "duplicate-exact",
        "template-long",
        "name-too-long",
        "unknown-key",
    ],
)
def test_operations_validation(overrides: dict[str, Any], loc: tuple[str | int, ...]) -> None:
    assert _rule_error_locs(**overrides) == [loc]


@pytest.mark.unit
def test_operations_rejects_unknown_top_level_keys() -> None:
    with pytest.raises(ValidationError) as caught:
        OperationRules.model_validate({"grpc": []})
    assert [error["loc"] for error in caught.value.errors()] == [("grpc",)]
