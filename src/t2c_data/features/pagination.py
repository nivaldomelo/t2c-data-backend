from __future__ import annotations

from math import ceil
from collections.abc import Sequence
from typing import TypeVar

from t2c_data.schemas.pagination import PageOut


T = TypeVar("T")
DEFAULT_PAGE_SIZE = 25
DEFAULT_MAX_PAGE_SIZE = 500


def normalize_page_params(
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    default_page_size: int = DEFAULT_PAGE_SIZE,
    max_page_size: int = DEFAULT_MAX_PAGE_SIZE,
) -> tuple[int, int]:
    normalized_page = max(int(page or 1), 1)
    bounded_max = max(int(max_page_size or DEFAULT_MAX_PAGE_SIZE), 1)
    bounded_default = max(min(int(default_page_size or DEFAULT_PAGE_SIZE), bounded_max), 1)
    normalized_page_size = max(min(int(page_size or bounded_default), bounded_max), 1)
    return normalized_page, normalized_page_size


def paginate_items(
    items: Sequence[T],
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_page_size: int = DEFAULT_MAX_PAGE_SIZE,
) -> PageOut[T]:
    normalized_page, normalized_page_size = normalize_page_params(
        page=page,
        page_size=page_size,
        default_page_size=DEFAULT_PAGE_SIZE,
        max_page_size=max_page_size,
    )
    total = len(items)
    total_pages = ceil(total / normalized_page_size) if total > 0 else 0
    start = (normalized_page - 1) * normalized_page_size
    end = start + normalized_page_size
    return PageOut[T](
        page=normalized_page,
        page_size=normalized_page_size,
        total=total,
        total_pages=total_pages,
        has_more=end < total,
        items=list(items)[start:end],
    )
