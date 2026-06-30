from __future__ import annotations

from t2c_data.features.pagination import normalize_page_params, paginate_items


def test_paginate_items_uses_default_page_size_and_has_more() -> None:
    items = list(range(26))

    page = paginate_items(items)

    assert page.page == 1
    assert page.page_size == 25
    assert page.total == 26
    assert page.has_more is True
    assert page.items == list(range(25))


def test_paginate_items_supports_second_page() -> None:
    items = list(range(26))

    page = paginate_items(items, page=2, page_size=25)

    assert page.page == 2
    assert page.page_size == 25
    assert page.total == 26
    assert page.has_more is False
    assert page.items == [25]


def test_paginate_items_caps_page_size_when_max_is_provided() -> None:
    items = list(range(300))

    page = paginate_items(items, page=1, page_size=250, max_page_size=100)

    assert page.page == 1
    assert page.page_size == 100
    assert page.total == 300
    assert page.has_more is True
    assert page.items == list(range(100))


def test_normalize_page_params_clamps_invalid_values() -> None:
    page, page_size = normalize_page_params(page=0, page_size=999, default_page_size=25, max_page_size=80)

    assert page == 1
    assert page_size == 80
