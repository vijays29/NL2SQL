from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    API_KEY: str
    DB_USER: str
    DB_PASS: str
    DB_HOST: str
    DB_PORT: str = "1521"
    DB_SERVICE_NAME: str
    
    DB_MIN_CONNECTIONS: int = 2
    DB_MAX_CONNECTIONS: int = 5
    DB_CONNECTION_INCREMENT: int = 1

    @classmethod
    def validate(cls):
        """
        Validates that required configuration values are present.
        """
        if not all([cls.API_KEY, cls.DB_USER, cls.DB_PASS, cls.DB_HOST, cls.DB_SERVICE_NAME]):
            raise ValueError("Missing required environment variables. Check .env file or environment.")

    class Config:
        env_file = ".env"
        extra = "allow" 

settings = Settings()