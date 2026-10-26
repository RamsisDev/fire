"""RouteTemplate and RouteTable: a concrete request path to the template that routes it."""

from __future__ import annotations

import pytest

from fire.core.domain.route_template import MAX_TEMPLATE_LENGTH, Route, RouteTable, RouteTemplate


def _table(*routes: tuple[str, str]) -> RouteTable:
    return RouteTable(Route(method, RouteTemplate.parse(template)) for method, template in routes)


def _matches(template: str, path: str) -> bool:
    return RouteTemplate.parse(template).matches(path.split("/")[1:])


@pytest.mark.unit
def test_parse_literals() -> None:
    parsed = RouteTemplate.parse("/v1/orders/search")
    assert parsed.template == "/v1/orders/search"
    assert parsed.literals == ("v1", "orders", "search")
    assert parsed.rank == (0, 0, 0)


@pytest.mark.unit
@pytest.mark.parametrize("placeholder", ["{order_id}", "{orderId}", "{a.b-c}", "{_id}"])
def test_parse_placeholders(placeholder: str) -> None:
    parsed = RouteTemplate.parse(f"/v1/orders/{placeholder}/logs")
    assert parsed.literals == ("v1", "orders", None, "logs")
    assert parsed.rank == (0, 0, 1, 0)


@pytest.mark.unit
def test_parse_root_and_trailing_slash() -> None:
    assert RouteTemplate.parse("/").literals == ("",)
    assert RouteTemplate.parse("/v1/orders/").literals == ("v1", "orders", "")


@pytest.mark.unit
def test_parse_accepts_max_length() -> None:
    template = "/" + "a" * (MAX_TEMPLATE_LENGTH - 1)
    assert RouteTemplate.parse(template).template == template


@pytest.mark.unit
@pytest.mark.parametrize(
    "template",
    [
        "",
        "v1/orders",
        "/" + "a" * MAX_TEMPLATE_LENGTH,
        "/files/{id}.json",
        "/files/v{version}",
        "/a{b",
        "/a}b",
        "/a b",
        "/a\tb",
        "/a\nb",
        "/a\x00b",
        "/a\x1fb",
        "/a\x7fb",
        "/a\x9fb",
        "/{}",
        "/{1st}",
        "/{order id}",
        "/{{id}}",
    ],
    ids=[
        "empty",
        "no-leading-slash",
        "too-long",
        "mixed-suffix",
        "mixed-prefix",
        "open-brace",
        "close-brace",
        "space",
        "tab",
        "newline",
        "nul",
        "unit-separator",
        "delete",
        "c1-control",
        "empty-placeholder",
        "placeholder-starts-with-digit",
        "placeholder-with-space",
        "nested-braces",
    ],
)
def test_parse_rejects(template: str) -> None:
    with pytest.raises(ValueError):
        RouteTemplate.parse(template)


@pytest.mark.unit
def test_placeholder_matches_one_non_empty_segment() -> None:
    assert _matches("/v1/orders/{order_id}", "/v1/orders/300000123456789")
    assert not _matches("/v1/orders/{order_id}", "/v1/orders/")
    assert not _matches("/v1/{kind}/{id}", "/v1//300000123456789")


@pytest.mark.unit
@pytest.mark.parametrize("path", ["/v1/orders", "/v1/orders/1/logs", "/v1/orders/1/"])
def test_segment_counts_must_be_equal(path: str) -> None:
    assert not _matches("/v1/orders/{order_id}", path)


@pytest.mark.unit
def test_trailing_slash_differs() -> None:
    assert not _matches("/v1/orders", "/v1/orders/")
    assert not _matches("/v1/orders/", "/v1/orders")
    assert _matches("/v1/orders/", "/v1/orders/")
    assert _matches("/", "/")
    assert not _matches("/", "//")


@pytest.mark.unit
@pytest.mark.parametrize("path", ["/v1/Orders/search", "/V1/orders/search", "/v1/orders/SEARCH"])
def test_literals_compare_exactly(path: str) -> None:
    assert _matches("/v1/orders/search", "/v1/orders/search")
    assert not _matches("/v1/orders/search", path)


@pytest.mark.unit
@pytest.mark.parametrize("reverse", [False, True], ids=["given-order", "reversed"])
def test_first_literal_where_templates_differ_wins(reverse: bool) -> None:
    routes = [("GET", "/a/{x}/c"), ("GET", "/a/b/{y}"), ("GET", "/{w}/b/c")]
    table = _table(*(reversed(routes) if reverse else routes))
    assert table.match("GET", "/a/b/c") == "/a/b/{y}"
    assert table.match("GET", "/a/z/c") == "/a/{x}/c"
    assert table.match("GET", "/z/b/c") == "/{w}/b/c"


@pytest.mark.unit
@pytest.mark.parametrize("reverse", [False, True], ids=["given-order", "reversed"])
def test_literal_sibling_beats_placeholder(reverse: bool) -> None:
    routes = [("GET", "/v1/orders/{order_id}"), ("GET", "/v1/orders/search")]
    table = _table(*(reversed(routes) if reverse else routes))
    assert table.match("GET", "/v1/orders/search") == "/v1/orders/search"
    assert table.match("GET", "/v1/orders/SO-1234567") == "/v1/orders/{order_id}"


@pytest.mark.unit
def test_equal_ranks_first_given_wins() -> None:
    assert _table(("GET", "/a/{x}"), ("GET", "/a/{y}")).match("GET", "/a/1") == "/a/{x}"
    assert _table(("GET", "/a/{y}"), ("GET", "/a/{x}")).match("GET", "/a/1") == "/a/{y}"


@pytest.mark.unit
def test_methods_match_exactly() -> None:
    table = _table(("get", "/a"), ("POST", "/b"))
    assert table.match("GET", "/a") == "/a"
    assert table.match("get", "/a") is None
    assert table.match("HEAD", "/a") is None
    assert table.match("GET", "/b") is None
    assert table.match("POST", "/b") == "/b"


@pytest.mark.unit
@pytest.mark.parametrize("method", ["BREW", "PURGE", ""])
def test_unknown_method_matches_nothing(method: str) -> None:
    assert _table(("GET", "/a"), ("GET", "/{x}")).match(method, "/a") is None


@pytest.mark.unit
@pytest.mark.parametrize("path", ["*", "", "a", "v1/orders/search"])
def test_path_without_leading_slash_matches_nothing(path: str) -> None:
    table = _table(("OPTIONS", "/{x}"), ("GET", "/v1/orders/search"))
    assert table.match("OPTIONS", path) is None
    assert table.match("GET", path) is None


@pytest.mark.unit
def test_unmatched_path() -> None:
    assert _table(("GET", "/v1/orders/{order_id}")).match("GET", "/v1/tasks/1") is None


@pytest.mark.unit
def test_len_counts_every_route() -> None:
    assert len(_table()) == 0
    assert _table().match("GET", "/") is None
    assert len(_table(("GET", "/a"), ("POST", "/a"), ("get", "/b"), ("GET", "/a"))) == 4
