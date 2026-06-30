from pydantic import BaseModel, Field


class TableTagsUpdateRequest(BaseModel):
    tag_ids: list[int] = Field(default_factory=list)


class TableGlossaryTermsUpdateRequest(BaseModel):
    term_ids: list[int] = Field(default_factory=list)
