from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    github_token: str = ""
    github_owner: str = ""
    fetch_interval_hours: int = 6
    db_path: str = "./data/repotrail.db"
    port: int = 8055
    api_key: str = ""

    @field_validator("github_owner")
    @classmethod
    def validate_owner(cls, v: str) -> str:
        if not v:
            raise ValueError("GITHUB_OWNER must not be empty")
        return v

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


@lru_cache
def get_settings() -> Settings:
    return Settings()
