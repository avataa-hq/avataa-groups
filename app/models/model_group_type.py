from typing import TYPE_CHECKING

from schemas.schema_group_type import GroupTypeSchema
from sqlalchemy.orm import Mapped, mapped_column, relationship
from store.db.base_class import Base

if TYPE_CHECKING:
    from .model_group import GroupModel
    from .model_group_template import GroupTemplateModel


class GroupTypeModel(Base):
    __tablename__ = "group_type"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    name: Mapped[str] = mapped_column(unique=True, init=False)

    groups: Mapped[list["GroupModel"]] = relationship(
        back_populates="group_type", cascade="all, delete", init=False
    )

    templates: Mapped[list["GroupTemplateModel"]] = relationship(
        back_populates="group_type_for_template",
        cascade="all, delete",
        init=False,
    )

    def to_schema(self) -> GroupTypeSchema:
        return GroupTypeSchema(
            id=self.id,
            name=self.name,
        )
