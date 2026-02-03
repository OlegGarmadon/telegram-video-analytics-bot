import asyncpg
import asyncio
from datetime import datetime
import os
from config import db_config

class Database:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        """Подключение к базе данных"""
        self.pool = await asyncpg.create_pool(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            database=db_config.name,
            min_size=1,
            max_size=10
        )
    
    async def create_tables(self):
        """Создание таблиц"""
        async with self.pool.acquire() as conn:
            # Таблица videos
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id BIGINT PRIMARY KEY,
                    creator_id BIGINT NOT NULL,
                    video_created_at TIMESTAMP NOT NULL,
                    views_count INTEGER NOT NULL DEFAULT 0,
                    likes_count INTEGER NOT NULL DEFAULT 0,
                    comments_count INTEGER NOT NULL DEFAULT 0,
                    reports_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица video_snapshots
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS video_snapshots (
                    id BIGINT PRIMARY KEY,
                    video_id BIGINT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
                    views_count INTEGER NOT NULL DEFAULT 0,
                    likes_count INTEGER NOT NULL DEFAULT 0,
                    comments_count INTEGER NOT NULL DEFAULT 0,
                    reports_count INTEGER NOT NULL DEFAULT 0,
                    delta_views_count INTEGER NOT NULL DEFAULT 0,
                    delta_likes_count INTEGER NOT NULL DEFAULT 0,
                    delta_comments_count INTEGER NOT NULL DEFAULT 0,
                    delta_reports_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Индексы для производительности
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(video_created_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_video ON video_snapshots(video_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_created ON video_snapshots(created_at)')
    
    async def execute_query(self, query: str):
        """Выполнение SQL запроса"""
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(query)
                return result
            except Exception as e:
                print(f"Ошибка выполнения запроса: {e}")
                print(f"Запрос: {query}")
                return None
    
    async def close(self):
        """Закрытие соединения"""
        if self.pool:
            await self.pool.close()

# Глобальный экземпляр БД
db = Database()