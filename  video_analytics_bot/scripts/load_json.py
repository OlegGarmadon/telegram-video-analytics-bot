import asyncio
import json
import sys
import os
from datetime import datetime

# Добавляем корневую директорию в путь Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import db_config
import asyncpg

async def load_json_to_db(json_path: str = "videos.json"):
    """Загрузка данных из JSON в базу данных"""
    
    if not os.path.exists(json_path):
        print(f"❌ Файл {json_path} не найден!")
        print("Пожалуйста, скачайте файл videos.json по ссылке из задания и поместите в корневую папку")
        return
    
    print(f"📂 Загрузка данных из {json_path}...")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"📊 Найдено {len(data)} видео для загрузки...")
    
    # Подключаемся к БД
    conn = await asyncpg.connect(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.name
    )
    
    try:
        video_count = 0
        snapshot_count = 0
        
        for video_data in data:
            # Вставляем видео
            await conn.execute('''
                INSERT INTO videos 
                (id, creator_id, video_created_at, views_count, likes_count, 
                 comments_count, reports_count, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO NOTHING
            ''',
                video_data['id'],
                video_data['creator_id'],
                datetime.fromisoformat(video_data['video_created_at'].replace('Z', '+00:00')),
                video_data['views_count'],
                video_data['likes_count'],
                video_data['comments_count'],
                video_data['reports_count'],
                datetime.fromisoformat(video_data['created_at'].replace('Z', '+00:00')),
                datetime.fromisoformat(video_data['updated_at'].replace('Z', '+00:00'))
            )
            video_count += 1
            
            # Загружаем снапшоты
            for snapshot in video_data.get('snapshots', []):
                await conn.execute('''
                    INSERT INTO video_snapshots 
                    (id, video_id, views_count, likes_count, comments_count, reports_count,
                     delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                     created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (id) DO NOTHING
                ''',
                    snapshot['id'],
                    video_data['id'],
                    snapshot['views_count'],
                    snapshot['likes_count'],
                    snapshot['comments_count'],
                    snapshot['reports_count'],
                    snapshot['delta_views_count'],
                    snapshot['delta_likes_count'],
                    snapshot['delta_comments_count'],
                    snapshot['delta_reports_count'],
                    datetime.fromisoformat(snapshot['created_at'].replace('Z', '+00:00')),
                    datetime.fromisoformat(snapshot['updated_at'].replace('Z', '+00:00'))
                )
                snapshot_count += 1
            
            # Показываем прогресс каждые 10 видео
            if video_count % 10 == 0:
                print(f"   Загружено {video_count} видео и {snapshot_count} снапшотов...")
        
        print(f"✅ Загрузка завершена!")
        print(f"   Всего загружено: {video_count} видео")
        print(f"   Всего загружено: {snapshot_count} снапшотов")
        
    finally:
        await conn.close()

async def main():
    """Основная функция"""
    print("🔗 Подключение к базе данных...")
    
    # Создаем подключение для создания таблиц
    conn = await asyncpg.connect(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.name
    )
    
    try:
        # Создаем таблицы
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
        
        # Создаем индексы
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_videos_created ON videos(video_created_at)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_video ON video_snapshots(video_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_snapshots_created ON video_snapshots(created_at)')
        
        print("✅ Таблицы созданы")
        
    finally:
        await conn.close()
    
    # Загружаем данные
    await load_json_to_db()
    
    print("🎉 Готово!")

if __name__ == "__main__":
    asyncio.run(main())