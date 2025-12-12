from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    GROQ_API_KEY: str
    DATABASE_URL: str

    # LLM Конфигурация
    LLM_PROVIDER: str = "openai"
    LLM_MODEL: str = "llama-3.3-70b-versatile"
    LLM_BASE_URL: str = "https://api.groq.com/openai/v1"
    LLM_TEMPERATURE: float = 0.1

    class Config:
        env_file = ".env"


settings = Settings()
