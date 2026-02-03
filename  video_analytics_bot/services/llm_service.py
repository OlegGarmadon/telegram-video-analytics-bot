import json
from typing import Dict, Any, Optional
import aiohttp
from pydantic import BaseModel
from config import config

class SQLQuery(BaseModel):
    query: str
    explanation: Optional[str] = None

class LLMService:
    def __init__(self):
        self.provider = config.llm.provider
        self.model = config.llm.openai_model
        
    async def generate_sql_from_text(self, user_query: str) -> SQLQuery:
        """Генерация SQL запроса из естественного языка"""
        
        schema_description = self._get_schema_description()
        
        prompt = f"""{schema_description}

Пользователь спрашивает: "{user_query}"

Твоя задача: сгенерировать SQL запрос для PostgreSQL, который вернет ОДНО ЧИСЛО (integer или float).

ВАЖНЫЕ ПРАВИЛА:
1. Возвращай ТОЛЬКО JSON объект с полями "query" и "explanation"
2. В поле "query" должен быть ВАЛИДНЫЙ SQL запрос
3. Запрос должен возвращать РОВНО ОДНУ колонку и ОДНУ строку
4. Используй только таблицы videos и video_snapshots
5. Даты: конвертируй "28 ноября 2025" в '2025-11-28'
6. Для периодов: "с 1 по 5 ноября 2025" -> BETWEEN '2025-11-01' AND '2025-11-05'
7. Числа: "100 000" -> 100000
8. Не используй LIMIT если не нужно ограничивать количество строк
9. Для подсчета уникальных видео используй COUNT(DISTINCT ...)

Примеры правильных запросов:
1. "Сколько всего видео?" → {{"query": "SELECT COUNT(*) FROM videos;", "explanation": "Счетчик всех видео"}}
2. "Сколько видео у креатора 123?" → {{"query": "SELECT COUNT(*) FROM videos WHERE creator_id = 123;", "explanation": "Количество видео креатора"}}
3. "Сумма просмотров за 28 ноября" → {{"query": "SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';", "explanation": "Суммарный прирост просмотров"}}

Теперь сгенерируй запрос для: "{user_query}"

Верни ТОЛЬКО JSON:"""
        
        if self.provider == 'openai':
            return await self._call_openai(prompt)
        elif self.provider == 'ollama':
            return await self._call_ollama(prompt)
        else:
            raise ValueError(f"Unknown provider: {self.provider}")
    
    def _get_schema_description(self) -> str:
        """Описание схемы базы данных для промпта"""
        return """
SCHEMA DESCRIPTION:

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
   - video_id (bigint) - ссылка на видео (foreign key к videos.id)
   - views_count (integer) - текущие просмотры на момент замера
   - likes_count (integer) - текущие лайки
   - comments_count (integer) - текущие комментарии
   - reports_count (integer) - текущие жалобы
   - delta_views_count (integer) - прирост просмотров за час
   - delta_likes_count (integer) - прирост лайков за час
   - delta_comments_count (integer) - прирост комментариев за час
   - delta_reports_count (integer) - прирост жалоб за час
   - created_at (timestamp) - время замера (раз в час)
   - updated_at (timestamp) - время обновления

ВАЖНО: delta_* поля показывают изменение за последний час от предыдущего замера.
Для суммы приростов за конкретный день используй SUM(delta_*) WHERE DATE(created_at) = 'дата'.
"""
    
    async def _call_openai(self, prompt: str) -> SQLQuery:
        """Вызов OpenAI API"""
        headers = {
            "Authorization": f"Bearer {config.llm.openai_api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "Ты эксперт по SQL и анализу данных."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,  # Низкая температура для детерминированных ответов
            "max_tokens": 500
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data
            ) as response:
                result = await response.json()
                content = result["choices"][0]["message"]["content"]
                
                # Парсинг JSON ответа
                try:
                    json_data = json.loads(content)
                    return SQLQuery(**json_data)
                except json.JSONDecodeError:
                    # Если LLM вернула не JSON, пытаемся извлечь SQL
                    return SQLQuery(
                        query=self._extract_sql_from_text(content),
                        explanation="Извлеченный SQL запрос"
                    )
    
    async def _call_ollama(self, prompt: str) -> SQLQuery:
        """Вызов локальной Ollama"""
        data = {
            "model": config.llm.ollama_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1}
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{config.llm.ollama_host}/api/generate",
                json=data
            ) as response:
                result = await response.json()
                content = result["response"]
                
                try:
                    json_data = json.loads(content)
                    return SQLQuery(**json_data)
                except json.JSONDecodeError:
                    return SQLQuery(
                        query=self._extract_sql_from_text(content),
                        explanation="Извлеченный SQL запрос"
                    )
    
    def _extract_sql_from_text(self, text: str) -> str:
        """Извлечение SQL запроса из текста"""
        import re
        # Ищем SQL запрос между ```sql ... ``` или SELECT ... ;
        sql_pattern = r"```sql\s*(.*?)\s*```|SELECT.*?;"
        matches = re.findall(sql_pattern, text, re.DOTALL | re.IGNORECASE)
        
        if matches:
            sql = matches[0] if isinstance(matches[0], str) else matches[1]
            return sql.strip()
        
        # Если не нашли, возвращаем пустой запрос
        return "SELECT 0;"