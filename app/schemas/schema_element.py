from pydantic import BaseModel


class ElementBase(BaseModel):
    entity_id: int

    def __hash__(self) -> int:
        return self.entity_id


# TODO Replace to [entity_id] and group_id
class ElementReadyToDB(ElementBase):
    group_id: int


class ElementInDB(ElementReadyToDB):
    id: int

    class Config:
        from_attributes = True


class ElementSchema(ElementInDB):
    pass


class ElementResponse(ElementInDB):
    description: bool
