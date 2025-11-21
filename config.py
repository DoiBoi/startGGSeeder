from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class DatabaseCredentials:
    url: str
    api_key: str
    email: str
    password: str

class EnvironmentConfig:
    """Loads and validates required environment configuration."""
    @staticmethod
    def load() -> DatabaseCredentials:
        database_url = os.getenv("DATABASE_API_URL")
        database_api = os.getenv("DATABASE_API_KEY")
        if not database_url or not database_api:
            raise ValueError("DATABASE_API_URL and DATABASE_API_KEY must be set in the environment variables.")

        email = os.getenv("DATABASE_LOGIN_EMAIL")
        password = os.getenv("DATABASE_LOGIN_PASSWORD")
        if not email or not password:
            raise ValueError("DATABASE_LOGIN_EMAIL and DATABASE_LOGIN_PASSWORD must be set in the environment variables.")

        return DatabaseCredentials(
            url=database_url,
            api_key=database_api,
            email=email,
            password=password,
        )
