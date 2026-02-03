from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class DatabaseConfig:
    host: str = os.getenv('DB_HOST', 'localhost')
    port: int = int(os.getenv('DB_PORT', 5432))
    name: str = os.getenv('DB_NAME', 'video_analytics')
    user: str = os.getenv('DB_USER', 'postgres')
    password: str = os.getenv('DB_PASSWORD', '')
    
    @property
    def dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

@dataclass
class BotConfig:
    token: str = os.getenv('BOT_TOKEN')
    
@dataclass
class LLMConfig:
    provider: str = os.getenv('LLM_PROVIDER', 'openai')
    openai_api_key: Optional[str] = os.getenv('OPENAI_API_KEY')
    openai_model: str = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
    ollama_host: str = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    ollama_model: str = os.getenv('OLLAMA_MODEL', 'llama2:13b')
    
@dataclass
class Config:
    db = DatabaseConfig()
    bot = BotConfig()
    llm = LLMConfig()

config = Config()