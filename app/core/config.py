from functools import lru_cache
from typing import Any, Literal, Self
from urllib.parse import urlunparse

from pydantic import (
    Field,
    PostgresDsn,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class APIConfig(BaseSettings):
    INVENTORY_HOST: str = Field(default="inventory")
    SEARCH_CLIENT_HOST: str = Field(default="search")


class CommonConfig(BaseSettings):
    LOGGING: int = Field(default=20, ge=0, le=50)
    LOG_WITH_TIME: bool = Field(default=False)
    DOCS_ENABLED: bool = Field(default=True)
    DOCS_CUSTOM_ENABLED: bool = Field(default=False)
    SWAGGER_JS_URL: str = Field(
        default="", validation_alias="DOCS_SWAGGER_JS_URL"
    )
    SWAGGER_CSS_URL: str = Field(
        default="", validation_alias="DOCS_SWAGGER_CSS_URL"
    )
    REDOC_JS_URL: str = Field(default="", validation_alias="DOCS_REDOC_JS_URL")


class DatabaseConfig(BaseSettings):
    schema_name: str = Field(default="public", alias="db_schema")
    db_type: str = Field(
        default="postgresql+asyncpg",
        alias="db_type",
    )
    user: str = Field(default="groups_admin")
    db_pass: str = Field(..., alias="db_pass")
    host: str = Field(default="pgbouncer")
    port: int = Field(default=5432)
    name: str = Field(default="group_builder_admin")

    @computed_field  # type: ignore
    @property
    def url(self) -> PostgresDsn:
        return PostgresDsn(
            f"{self.db_type}://{self.user}:{self.db_pass}@{self.host}:{self.port}/{self.name}",
        )

    model_config = SettingsConfigDict(env_prefix="db_")


class GRPCConfig(BaseSettings):
    INVENTORY_GRPC_PORT: int = Field(default=50051, ge=1, le=65_535)
    SEARCH_GRPC_PORT: int = Field(default=50051, ge=1, le=65_535)
    SERVER_GRPC_PORT: int = Field(default=50051, ge=1, le=65_535)


class KeycloakConfig(BaseSettings):
    realm: str = Field(
        default="avataa",
        min_length=1,
        serialization_alias="realm_name",
    )
    client_id: str = Field(default="kafka", min_length=1)
    client_secret: str = Field(
        ...,
        serialization_alias="client_secret_key",
    )
    scope: str = Field(default="profile", min_length=1)
    protocol: Literal["http", "https"] = Field(default="http")
    host: str = Field(default="keycloak", min_length=1)
    port: int = Field(default=8080, ge=1, le=65_535)

    @computed_field  # type: ignore
    @property
    def url(self) -> str:
        url = urlunparse(
            (
                str(self.protocol),
                f"{self.host}:{self.port}",
                "auth",
                "",
                "",
                "",
            )
        )
        return url

    model_config = SettingsConfigDict(env_prefix="keycloak_")


class KafkaConfig(BaseSettings):
    # Config example for correct work Kafka client
    # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    turn_on: bool = Field(default=True)
    secured: bool = Field(default=False, validation_alias="kafka_with_keycloak")
    topic: str = Field(default="group")
    group_data_changes_proto: str = Field(default="group_data.changes")
    inventory_topic: str = Field(default="inventory.changes")
    bootstrap_servers: str = Field(
        "kafka:9092",
        serialization_alias="bootstrap.servers",
        validation_alias="kafka_url",
        min_length=1,
    )
    group_id: str = Field(
        "Group_Builder",
        validation_alias="kafka_consumer_name",
        serialization_alias="group.id",
        min_length=1,
    )
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        "latest",
        serialization_alias="auto.offset.reset",
        validation_alias="kafka_offset",
    )
    enable_auto_commit: bool = Field(
        False, serialization_alias="enable.auto.commit"
    )
    sasl_mechanism: Literal["OAUTHBEARER", None] = Field(
        None, serialization_alias="sasl.mechanisms"
    )
    security_protocol_raw: Literal[
        "plaintext", "sasl_plaintext", "sasl_ssl", "ssl", None
    ] = Field(None, validation_alias="kafka_security_protocol")

    log_level: int = Field(default=20, ge=0, le=50, validation_alias="logging")

    @field_validator("security_protocol_raw", mode="before")
    @classmethod
    def normalize_security_protocol(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.lower()
        else:
            return value

    @computed_field  # type: ignore
    @property
    def debug(self) -> str | None:
        if self.log_level <= 10:
            return "security,broker,protocol"
        return None

    # @computed_field  # type: ignore
    # @property
    # def oauth_cb(
    #     self: Self,
    # ) -> None | Callable[[None, KeycloakConfig], tuple[str, float]]:
    #     if not self.secured:
    #         return None
    #     return partial(
    #         self._get_token_for_kafka, keycloak_config=KeycloakConfig()
    #     )

    @computed_field  # type: ignore
    @property
    def security_protocol(self) -> str:
        if self.secured:
            return str(self.security_protocol_raw) or "sasl_plaintext"
        return "plaintext"

    # def _get_token_for_kafka(
    #     self, conf: Any, keycloak_config: KeycloakConfig
    # ) -> tuple[str, float]:
    #     keycloak_openid = KeycloakOpenID(
    #         server_url=keycloak_config.url,
    #         client_id=keycloak_config.client_id,
    #         realm_name=keycloak_config.realm,
    #         client_secret_key=keycloak_config.client_secret,
    #     )
    #     attempt = 5
    #     token = ""
    #     expires_in = 1.0
    #     while attempt > 0:
    #         try:
    #             tkn = keycloak_openid.token(grant_type="client_credentials")
    #             token = tkn["access_token"]
    #             expires_in = float(tkn["expires_in"]) * 0.95
    #         except Exception as ex:
    #             print(ex)
    #             time.sleep(1)
    #             attempt -= 1
    #         else:
    #             if tkn:
    #                 break
    #             time.sleep(1)
    #             attempt -= 1
    #             continue
    #     # print(
    #     #     f"KEYCLOAK TOKEN FOR KAFKA: ...{tkn['access_token'][-3:]} EXPIRED_TIME:{expires_in}."
    #     # )
    #     return token, time.time() + expires_in

    def model_dump(self: Self, **kwargs: Any) -> dict[str, Any]:
        data = super().model_dump(**kwargs)
        data["security.protocol"] = self.security_protocol
        return data

    model_config = SettingsConfigDict(env_prefix="kafka_")


class RedisConfig(BaseSettings):
    host: str = Field(default="redis")
    port: int = Field(default=6379, ge=1, le=65_535)
    password: str = Field(default="", validation_alias="redis_pass")

    model_config = SettingsConfigDict(env_prefix="redis_")


class BufferedMoConfig(BaseSettings):
    KAFKA_BUFFER_TIMEOUT_SEC: int = Field(default=15, ge=0, le=600)


class SecurityConfig(BaseSettings):
    admin_role: str = Field(default="__admin")
    security_type: str = Field(default="KEYCLOAK-INFO")

    @field_validator("security_type", mode="before")
    @classmethod
    def normalize_security_type(cls, value: str) -> str:
        if isinstance(value, str):
            return value.upper()
        else:
            return value

    keycloak_protocol: Literal["http", "https"] = Field(default="http")
    keycloak_host: str = Field(
        default="localhost", min_length=1, validation_alias="keycloak_host"
    )
    keycloak_port: int | None = Field(
        default=None, ge=1, le=65_535, validation_alias="keycloak_port"
    )
    keycloak_redirect_protocol_raw: Literal["http", "https", None] = Field(
        default=None, validation_alias="keycloak_redirect_protocol"
    )
    keycloak_redirect_host_raw: str | None = Field(
        default=None, min_length=1, validation_alias="keycloak_redirect_host"
    )
    keycloak_redirect_port_raw: int | None = Field(
        default=None, ge=1, le=65_535, validation_alias="keycloak_redirect_port"
    )
    realm: str = Field(
        default="avataa", min_length=1, validation_alias="keycloak_realm"
    )

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_protocol(self) -> str:
        if self.keycloak_redirect_protocol_raw is None:
            return self.keycloak_protocol
        return self.keycloak_redirect_protocol_raw

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_host(self) -> str:
        if self.keycloak_redirect_host_raw is None:
            return self.keycloak_host
        return self.keycloak_redirect_host_raw

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_port(self) -> int:
        if self.keycloak_redirect_port_raw is None:
            return self.keycloak_port
        return self.keycloak_redirect_port_raw

    @computed_field  # type: ignore
    @property
    def keycloak_url(self) -> str:
        url = f"{self.keycloak_protocol}://{self.keycloak_host}"
        if self.keycloak_port:
            url = f"{url}:{self.keycloak_port}"
        return url

    @computed_field  # type: ignore
    @property
    def keycloak_public_key_url(self) -> str:
        return f"{self.keycloak_url}/realms/{self.realm}"

    @computed_field  # type: ignore
    @property
    def keycloak_redirect_url(self) -> str:
        url = (
            f"{self.keycloak_redirect_protocol}://{self.keycloak_redirect_host}"
        )
        if self.keycloak_redirect_port:
            url = f"{url}:{self.keycloak_redirect_port}"
        return url

    @computed_field  # type: ignore
    @property
    def keycloak_token_url(self) -> str:
        return f"{self.keycloak_redirect_url}/realms/{self.realm}/protocol/openid-connect/token"

    @computed_field  # type: ignore
    @property
    def keycloak_authorization_url(self) -> str:
        return f"{self.keycloak_redirect_url}/realms/{self.realm}/protocol/openid-connect/auth"

    opa_protocol: Literal["http", "https"] = Field(default="http")
    opa_host: str = Field(default="opa", min_length=1)
    opa_port: int = Field(default=8181, ge=1, le=65_535)
    opa_policy: str = Field(default="main")

    @computed_field  # type: ignore
    @property
    def opa_url(self) -> str:
        return f"{self.opa_protocol}://{self.opa_host}:{self.opa_port}"

    @computed_field  # type: ignore
    @property
    def opa_policy_path(self) -> str:
        return f"/v1/data/{self.opa_policy}"

    security_middleware_protocol: Literal["http", "https"] = Field(
        default="http", validation_alias="security_middleware_protocol"
    )
    security_middleware_host: str = Field(
        default="security-middleware", min_length=1
    )
    security_middleware_port: int = Field(default=8000, ge=1, le=65535)

    @model_validator(mode="after")
    def set_defaults(self) -> "SecurityConfig":
        if self.security_middleware_protocol is None:
            self.security_middleware_protocol = self.keycloak_protocol
        if self.security_middleware_host is None:
            self.security_middleware_host = self.keycloak_host
        if self.security_middleware_port is None:
            self.security_middleware_port = self.keycloak_port
        return self

    @computed_field  # type: ignore
    @property
    def security_postfix(self) -> str:
        if (
            self.security_middleware_host == self.keycloak_host
            and self.security_middleware_port == self.keycloak_port
        ):
            url = f"/realms/{self.realm}/protocol/openid-connect/userinfo"
        else:
            url = f"/api/security_middleware/v1/cached/realms/{self.realm}/protocol/openid-connect/userinfo"
        return url

    @computed_field  # type: ignore
    @property
    def security_middleware_url(self) -> str:
        return (
            f"{self.security_middleware_protocol}://{self.security_middleware_host}:{self.security_middleware_port}"
            f"{self.security_postfix}"
        )


class Config(BaseSettings):
    api: APIConfig = Field(default_factory=APIConfig)
    common: CommonConfig = Field(default_factory=CommonConfig)
    db: DatabaseConfig = Field(default_factory=DatabaseConfig)

    grpc: GRPCConfig = Field(default_factory=GRPCConfig)
    keycloak: KeycloakConfig = Field(default_factory=KeycloakConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    buffered_mo: BufferedMoConfig = Field(default_factory=BufferedMoConfig)
    security_config: SecurityConfig = Field(default_factory=SecurityConfig)


@lru_cache
def setup_config() -> Config:
    settings = Config()
    return settings
