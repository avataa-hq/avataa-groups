from pydantic import BaseModel


class GroupTypeSchemaBase(BaseModel):
    name: str


class GroupTypeSchemaAdd(GroupTypeSchemaBase):
    pass


class GroupTypeSchemaEdit(GroupTypeSchemaBase):
    pass


class GroupTypeSchemaDelete(GroupTypeSchemaBase):
    pass


class GroupTypeInDB(GroupTypeSchemaBase):
    id: int  # noqa: A003

    # groups: GroupInDB

    class Config:
        from_attributes = True


class GroupTypeSchema(GroupTypeInDB):
    pass
