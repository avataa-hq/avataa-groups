from typing import TYPE_CHECKING

from schemas.schema_group import GroupSchema
from sqlalchemy import JSON, BigInteger, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from store.db.base_class import Base

if TYPE_CHECKING:
    from .model_element import ElementModel
    from .model_group_template import GroupTemplateModel
    from .model_group_type import GroupTypeModel


class GroupModel(Base):
    __tablename__ = "group"

    id: Mapped[int] = mapped_column(primary_key=True)
    group_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    group_process_instance_key: Mapped[int] = mapped_column(
        BigInteger, nullable=True, unique=True
    )
    tmo_id: Mapped[int] = mapped_column(nullable=False)
    is_valid: Mapped[bool] = mapped_column(nullable=True)
    column_filters: Mapped[list] = mapped_column(JSON, nullable=True)
    ranges_object: Mapped[dict] = mapped_column(JSON, nullable=True)

    group_type_id: Mapped[int] = mapped_column(
        ForeignKey("group_type.id", ondelete="CASCADE")
    )
    group_type: Mapped["GroupTypeModel"] = relationship(
        back_populates="groups", lazy="immediate"
    )

    group_template_id: Mapped[int] = mapped_column(
        ForeignKey("group_template.id", ondelete="CASCADE"), nullable=True
    )
    group_template: Mapped["GroupTemplateModel"] = relationship(
        back_populates="groups_for_template"
    )

    elements: Mapped[list["ElementModel"]] = relationship(
        back_populates="group", cascade="all, delete", lazy="immediate"
    )
    min_qnt: Mapped[int] = mapped_column(nullable=True)
    is_aggregate: Mapped[bool] = mapped_column(default=True, server_default="t")

    def __str__(self):
        return f"{self.__class__.__name__}({self.id=}, {self.group_name=}, {self.tmo_id=})"

    def __repr__(self):
        return str(self)

    def to_schema(self, delete: bool = False) -> GroupSchema:
        if delete or not self.elements:
            schema_elements = []
        else:
            schema_elements = [element.to_schema() for element in self.elements]

        return GroupSchema(
            id=self.id,
            group_name=self.group_name,
            group_type_id=self.group_type_id,
            group_process_instance_key=self.group_process_instance_key,
            tmo_id=self.tmo_id,
            is_valid=self.is_valid,
            column_filters=self.column_filters,
            ranges_object=self.ranges_object,
            is_aggregate=self.is_aggregate,
            min_qnt=self.min_qnt,
            group_template_id=self.group_template_id,
            elements=schema_elements,
            group_type=self.group_type.to_schema(),
        )
