from typing import TYPE_CHECKING

from schemas.schema_element import ElementSchema
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from store.db.base_class import Base

if TYPE_CHECKING:
    from .model_group import GroupModel


class ElementModel(Base):
    __tablename__ = "element"
    __table_args__ = (UniqueConstraint("entity_id", "group_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)

    # Non-unique value
    entity_id: Mapped[int] = mapped_column(nullable=False)

    group_id: Mapped[int] = mapped_column(
        ForeignKey("group.id", ondelete="CASCADE")
    )
    group: Mapped["GroupModel"] = relationship(back_populates="elements")

    def __str__(self):
        return f"{self.__class__.__name__}({self.id=}, {self.entity_id=})"

    def __repr__(self):
        return str(self)

    def to_schema(self) -> ElementSchema:
        return ElementSchema(
            id=self.id,
            entity_id=self.entity_id,
            group_id=self.group_id,
        )
