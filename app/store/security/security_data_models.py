from dataclasses import dataclass
from typing import List, Optional, Union

from core.config import setup_config
from pydantic import BaseModel


@dataclass
class ClientRoles:
    name: str
    roles: List[str]


@dataclass
class UserData:
    id: Optional[str]
    audience: Optional[Union[List[str], str]]
    name: str
    preferred_name: str
    realm_access: Optional[ClientRoles]
    resource_access: Optional[List[ClientRoles]]
    groups: Optional[List[str]]

    @classmethod
    def from_jwt(cls, jwt: dict):
        realm_access = jwt.get("realm_access", None)
        if realm_access is not None:
            realm_access = ClientRoles(
                name="realm_access", roles=realm_access.get("roles", [])
            )
        resource_access = jwt.get("resource_access", None)
        if resource_access is not None:
            resource_access = [
                ClientRoles(name=k, roles=v.get("roles", []))
                for k, v in resource_access.items()
            ]

        return cls(
            id=jwt.get("sub", None),
            audience=jwt.get("aud", None),
            name=f"""{jwt.get("given_name", "")} {jwt.get("family_name", "")}""",
            preferred_name=jwt.get("preferred_username", jwt.get("upn", "")),
            realm_access=realm_access,
            resource_access=resource_access,
            groups=jwt.get("groups", None),
        )


class UserPermission(BaseModel):
    is_admin: bool = False
    user_permissions: List[str] | None = None


class UserPermissionBuilder:
    def __init__(self, user_data: UserData):
        self.user_data = user_data
        self.admin_role = setup_config().security_config.admin_role

    def get_user_permissions(self) -> UserPermission:
        is_admin = self.__check_permission_is_admin()
        user_permissions = self.__get_permissions_from_client_role()
        return UserPermission(
            is_admin=is_admin, user_permissions=user_permissions
        )

    def __check_permission_is_admin(self) -> bool:
        """Returns True if client has admin permissions"""
        client_role = self.user_data.realm_access
        if not client_role:
            return False
        user_roles = set(client_role.roles)
        if self.admin_role in user_roles:
            return True
        return False

    def __get_permissions_from_client_role(self) -> List[str]:
        """Returns a list of permissions as they store in elasticsearch"""
        client_role = self.user_data.realm_access
        if not client_role:
            return list()
        permissions_to_search = [
            f"{client_role.name}.{role}" for role in client_role.roles
        ]
        return permissions_to_search
