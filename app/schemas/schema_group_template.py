from pydantic import BaseModel, Field, StringConstraints, model_validator
from typing_extensions import Annotated, Self


class GroupTemplateBase(BaseModel):
    name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    column_filters: list
    ranges_object: dict | None = None
    identical: list[int] | None = None
    min_qnt: Annotated[
        int, Field(strict=True, ge=0, le=2_147_483_646, default=0)
    ]
    tmo_id: Annotated[int, Field(strict=True, ge=1, le=2_147_483_646)]

    @model_validator(mode="after")
    def check_at_least_one_not_empty(self) -> Self:
        if not (self.column_filters or self.ranges_object or self.identical):
            raise ValueError(
                "At least one of column_filters, ranges_object, or identical must be non-empty"
            )
        return self


class GroupTemplateMain(GroupTemplateBase):
    group_type_id_for_template: Annotated[
        int, Field(strict=True, ge=1, le=2_147_483_646)
    ]


class GroupTemplateCreate(GroupTemplateBase):
    group_type_name: str


class GroupTemplateInDB(GroupTemplateMain):
    id: int

    class Config:
        from_attributes = True


class GroupTemplateSchema(GroupTemplateInDB):
    pass
