from pydantic import Field, PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TestsConfig(BaseSettings):
    run_container_postgres_local: bool = Field(
        default=True,
        alias="tests_run_container_postgres_local",
    )
    db_type: str = Field(
        default="postgresql+asyncpg",
        alias="tests_db_type",
    )
    user: str = Field(default="test_user")
    db_pass: str = Field(default="password", alias="tests_db_pass")
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    name: str = Field(default="templates")
    docker_db_host: str = Field(
        default="localhost",
        alias="test_docker_db_host",
    )

    @computed_field  # type: ignore
    @property
    def url(self) -> PostgresDsn:
        return PostgresDsn(
            f"{self.db_type}://{self.user}:{self.db_pass}@{self.host}:{self.port}/{self.name}",
        )

    model_config = SettingsConfigDict(env_prefix="tests_db_")
