from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, Field


T = TypeVar("T")


class PageOut(BaseModel, Generic[T]):
    page: int
    page_size: int
    total: int
    total_pages: int = 0
    has_more: bool = False
    items: list[T] = Field(default_factory=list)
