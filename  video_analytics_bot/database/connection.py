from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text
from config import config

class Database:
    def __init__(self):
        self.engine = create_async_engine(
            config.db.dsn,
            echo=False,  # Включить для дебага SQL-запросов
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
    
    async def create_tables(self):
        """Создание таблиц в БД"""
        from database.models import Base
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def get_session(self) -> AsyncSession:
        """Получение сессии БД"""
        async with self.session_factory() as session:
            yield session
    
    async def execute_query(self, query: str) -> list:
        """Выполнение SQL-запроса"""
        async with self.session_factory() as session:
            result = await session.execute(text(query))
            return result.fetchall()

db = Database()