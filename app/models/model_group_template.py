from typing import TYPE_CHECKING

from schemas.schema_group_template import GroupTemplateSchema
from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from store.db.base_class import Base

if TYPE_CHECKING:
    from .model_group import GroupModel
    from .model_group_type import GroupTypeModel


class GroupTemplateModel(Base):
    __tablename__ = "group_template"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(
        String(255),
        unique=True,
    )
    column_filters: Mapped[list] = mapped_column(JSON, nullable=True)
    ranges_object: Mapped[dict] = mapped_column(JSON, nullable=True)
    identical: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    min_qnt: Mapped[int] = mapped_column(nullable=False)
    tmo_id: Mapped[int] = mapped_column(nullable=False)

    groups_for_template: Mapped[list["GroupModel"]] = relationship(
        back_populates="group_template", cascade="all, delete"
    )

    group_type_id_for_template: Mapped[int] = mapped_column(
        ForeignKey("group_type.id", ondelete="CASCADE")
    )
    group_type_for_template: Mapped["GroupTypeModel"] = relationship(
        back_populates="templates"
    )

    def __str__(self):
        return f"{self.__class__.__name__}({self.id=}, {self.name=})"

    def __repr__(self):
        return str(self)

    def to_schema(self) -> GroupTemplateSchema:
        return GroupTemplateSchema(
            id=self.id,
            name=self.name,
            column_filters=self.column_filters,
            ranges_object=self.ranges_object,
            identical=self.identical,
            min_qnt=self.min_qnt,
            tmo_id=self.tmo_id,
            group_type_id_for_template=self.group_type_id_for_template,
        )
