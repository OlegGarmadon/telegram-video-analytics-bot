import openai
import json
import re
import os
from typing import Tuple
from config import llm_config

class LLMService:
    def __init__(self):
        openai.api_key = llm_config.openai_api_key
    
    async def generate_sql_from_text(self, user_query: str) -> Tuple[str, str]:
        """Генерация SQL запроса из естественного языка"""
        
        schema = """
Таблицы в базе данных PostgreSQL:

1. Таблица videos (итоговая статистика по видео):
   - id (bigint) - идентификатор видео
   - creator_id (bigint) - идентификатор креатора
   - video_created_at (timestamp) - дата публикации видео
   - views_count (integer) - финальное количество просмотров
   - likes_count (integer) - финальное количество лайков
   - comments_count (integer) - финальное количество комментариев
   - reports_count (integer) - финальное количество жалоб
   - created_at (timestamp) - время создания записи
   - updated_at (timestamp) - время обновления

2. Таблица video_snapshots (почасовые замеры):
   - id (bigint) - идентификатор снапшота
   - video_id (bigint) - ссылка на видео
   - views_count (integer) - текущие просмотры
   - likes_count (integer) - текущие лайки
   - comments_count (integer) - текущие комментарии
   - reports_count (integer) - текущие жалобы
   - delta_views_count (integer) - прирост просмотров за час
   - delta_likes_count (integer) - прирост лайков за час
   - delta_comments_count (integer) - прирост комментариев за час
   - delta_reports_count (integer) - прирост жалоб за час
   - created_at (timestamp) - время замера
   - updated_at (timestamp) - время обновления
        """
        
        examples = """
Примеры преобразования:
1. "Сколько всего видео?" → SELECT COUNT(*) FROM videos;
2. "Сколько видео у креатора 123?" → SELECT COUNT(*) FROM videos WHERE creator_id = 123;
3. "Сколько видео набрало больше 100000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000;
4. "На сколько просмотров выросли все видео 28 ноября 2025?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';
5. "Сколько разных видео получали новые просмотры 27 ноября 2025?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
        """
        
        prompt = f"""{schema}

{examples}

Пользователь спрашивает: "{user_query}"

Сгенерируй SQL запрос для PostgreSQL, который вернет ОДНО ЧИСЛО.
Запрос должен быть простым и эффективным.

SQL запрос:"""
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Ты эксперт по SQL и анализу данных. Генерируй только SQL код."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=200
            )
            
            sql = response.choices[0].message.content.strip()
            
            # Очищаем SQL от лишних символов
            sql = sql.replace('```sql', '').replace('```', '').strip()
            
            # Убеждаемся, что запрос заканчивается точкой с запятой
            if not sql.endswith(';'):
                sql += ';'
            
            return sql, "Сгенерирован запрос"
            
        except Exception as e:
            print(f"Ошибка OpenAI: {e}")
            # Возвращаем простой запрос по умолчанию
            return "SELECT COUNT(*) FROM videos;", "Ошибка, используется запрос по умолчанию"