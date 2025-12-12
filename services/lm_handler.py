import aiohttp
import logging
from typing import Dict, Any

from db.config import settings

logger = logging.getLogger(__name__)


class LLMHandler:
    def __init__(self):
        self.api_key = settings.GROQ_API_KEY
        self.base_url = settings.LLM_BASE_URL
        self.model = settings.LLM_MODEL
        self.temperature = settings.LLM_TEMPERATURE

    async def generate_sql_query(self, question: str, context: Dict[str, Any] = None) -> str:
        """Генерация SQL запроса на основе естественного языка"""
        try:
            # Промт!
            system_prompt = """Ты эксперт по SQL и анализу данных. 
            Тебе нужно преобразовать вопросы на русском языке в SQL запросы для PostgreSQL.

            Структура базы данных:
            1. Таблица videos:
               - id (UUID) - уникальный идентификатор видео
               - creator_id (VARCHAR) - идентификатор креатора
               - video_created_at (TIMESTAMP) - когда было создано видео
               - views_count (INTEGER) - количество просмотров
               - likes_count (INTEGER) - количество лайков
               - comments_count (INTEGER) - количество комментариев
               - reports_count (INTEGER) - количество репортов
               - created_at (TIMESTAMP) - когда запись создана в системе
               - updated_at (TIMESTAMP) - когда запись обновлена

            2. Таблица video_snapshots:
               - id (UUID) - уникальный идентификатор снапшота
               - video_id (UUID) - ссылка на видео
               - views_count (INTEGER) - просмотры на момент снапшота
               - likes_count (INTEGER) - лайки на момент снапшота
               - comments_count (INTEGER) - комментарии на момент снапшота
               - reports_count (INTEGER) - репорты на момент снапшота
               - delta_views_count (INTEGER) - изменение просмотров
               - delta_likes_count (INTEGER) - изменение лайков
               - delta_comments_count (INTEGER) - изменение комментариев
               - delta_reports_count (INTEGER) - изменение репортов
               - created_at (TIMESTAMP) - когда снапшот создан
               - updated_at (TIMESTAMP) - когда снапшот обновлен

            Важные правила:
            1. Всегда используй правильные имена таблиц и полей
            2. Для дат используй TIMESTAMP WITH TIME ZONE
            3. Для подсчета используй COUNT(*)
            4. Для суммирования используй SUM()
            5. Для фильтрации по дате используй WHERE DATE(created_at) BETWEEN ...
            6. Возвращай ТОЛЬКО SQL запрос без пояснений

            Примеры:
            Вопрос: "Сколько всего видео есть в системе?"
            SQL: SELECT COUNT(*) FROM videos;

            Вопрос: "Сколько видео у креатора с id abc вышло с 1 по 5 ноября 2025?"
            SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

            Вопрос: "Сколько видео набрало больше 100000 просмотров?"
            SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

            Вопрос: "На сколько просмотров выросли все видео 28 ноября 2025?"
            SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

            Вопрос: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
            SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
            """

            # Подготовка сообщений для Groq
            messages = [
                {"role": "system", "content": str(system_prompt)},
                {"role": "user", "content": str(question)}
            ]

            safe_context = None
            if context:
                safe_context = {k: str(v) for k, v in context.items()}
            if safe_context:
                messages.append({
                    "role": "system",
                    "content": f"Контекст: {safe_context}"
                })

            # Формирование запроса для Groq
            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": self.temperature,
                "max_tokens": 500
            }

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            # Запрос к Groq
            # logger.debug(f"[LLM] URL запроса: {self.base_url}/chat/completions")
            # logger.debug(f"[LLM] Заголовки запроса: {headers}")
            # logger.debug(f"[LLM] Отправляемый payload: {payload}")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        f"{self.base_url}/chat/completions",
                        headers=headers,
                        json=payload
                ) as response:
                    # raw_text = await response.text()
                    # logger.debug(f"[LLM] Статус ответа: {response.status}")
                    # logger.debug(f"[LLM] Сырой ответ от модели: {raw_text}")

                    if response.status == 200:
                        result = await response.json()
                        sql_query = result['choices'][0]['message']['content'].strip()

                        # Очищаем SQL запрос лишних символов
                        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()

                        logger.info(f"Сгенерирован SQL: {sql_query}")
                        return sql_query
                    else:
                        error_text = await response.text()
                        # logger.error(f"Ошибка Groq API: {response.status} - {error_text}")
                        raise Exception(f"Ошибка: {response.status} - {error_text}")

        except Exception as e:
            logger.error(f"Ошибка генерации SQL: {e}")
            return None