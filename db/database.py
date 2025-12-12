import asyncpg
import logging
import json
from typing import Any, Dict, List, Optional
from datetime import datetime


logger = logging.getLogger(__name__)


class Database:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Создание пула соединений"""
        self.pool = await asyncpg.create_pool(self.connection_string)
        logger.info("Пул соединений с базой данных создан")

    async def disconnect(self):
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            logger.info("Пул соединений с базой данных закрыт")

    async def execute_query(self, query: str, *args) -> List[Dict[str, Any]]:
        """Выполнение SQL запроса и возврат списка словарей"""
        async with self.pool.acquire() as connection:
            try:
                rows = await connection.fetch(query, *args)
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Ошибка при выполнении SQL-запроса: {e}")
                raise

    async def execute_scalar(self, query: str, *args) -> Any:
        """Выполнение SQL запроса и возврат скалярного значения"""
        async with self.pool.acquire() as connection:
            try:
                result = await connection.fetchval(query, *args)
                return result
            except Exception as e:
                logger.error(f"Ошибка при выполнении scalar-запроса: {e}")
                raise

    async def load_json_data(self, json_path: str):
        """Загрузка данных из JSON файла в базу"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Извлекаем список видео
            if isinstance(data, dict) and 'videos' in data:
                videos_list = data['videos']
                logger.info(f"Обнаружено {len(videos_list)} видео в JSON объекте")
            elif isinstance(data, list):
                videos_list = data
                logger.info(f"Обнаружено {len(videos_list)} видео в JSON массиве")
            else:
                logger.error("Неподдерживаемый формат JSON: ожидается ключ 'videos' или массив")
                return

            logger.info(f"Начинается загрузка {len(videos_list)} видео в базу данных")

            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    # Очистка таблиц
                    await connection.execute("DELETE FROM video_snapshots")
                    await connection.execute("DELETE FROM videos")

                    videos_loaded = 0
                    snapshots_loaded = 0

                    for video in videos_list:
                        try:
                            # Преобразуем даты
                            video_created_at = self._parse_datetime_naive(video['video_created_at'])
                            created_at = self._parse_datetime_naive(video['created_at'])
                            updated_at = self._parse_datetime_naive(video['updated_at'])

                            # Вставка видео
                            await connection.execute("""
                                INSERT INTO videos (
                                    id, creator_id, video_created_at,
                                    views_count, likes_count, comments_count, reports_count,
                                    created_at, updated_at
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            """,
                                str(video['id']),
                                str(video['creator_id']),
                                video_created_at,
                                video.get('views_count', 0),
                                video.get('likes_count', 0),
                                video.get('comments_count', 0),
                                video.get('reports_count', 0),
                                created_at,
                                updated_at
                            )
                            videos_loaded += 1

                            # Снапшоты
                            snapshots = video.get('snapshots', [])
                            for snapshot in snapshots:
                                snapshot_created_at = self._parse_datetime_naive(snapshot['created_at'])
                                snapshot_updated_at = self._parse_datetime_naive(snapshot['updated_at'])

                                await connection.execute("""
                                    INSERT INTO video_snapshots (
                                        id, video_id,
                                        views_count, likes_count, comments_count, reports_count,
                                        delta_views_count, delta_likes_count,
                                        delta_comments_count, delta_reports_count,
                                        created_at, updated_at
                                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                """,
                                    str(snapshot['id']),
                                    str(video['id']),
                                    snapshot.get('views_count', 0),
                                    snapshot.get('likes_count', 0),
                                    snapshot.get('comments_count', 0),
                                    snapshot.get('reports_count', 0),
                                    snapshot.get('delta_views_count', 0),
                                    snapshot.get('delta_likes_count', 0),
                                    snapshot.get('delta_comments_count', 0),
                                    snapshot.get('delta_reports_count', 0),
                                    snapshot_created_at,
                                    snapshot_updated_at
                                )
                                snapshots_loaded += 1

                        except KeyError as e:
                            logger.warning(f"Отсутствует обязательное поле {e} в видео {video.get('id', 'неизвестно')}")
                        except Exception as e:
                            logger.error(f"Ошибка при обработке видео {video.get('id', 'неизвестно')}: {e}")

                    logger.info(f"Успешно загружено: {videos_loaded} видео, {snapshots_loaded} снапшотов")

        except FileNotFoundError:
            logger.error(f"Файл не найден: {json_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка формата JSON в файле {json_path}: {e}")
            raise
        except UnicodeDecodeError:
            logger.error(f"Ошибка кодировки файла: {json_path}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке данных: {e}")
            raise

    def _parse_datetime_naive(self, dt_str: str):
        """Парсинг строки даты и удаление временной зоны"""
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1]

        if '+' in dt_str:
            dt_str = dt_str.split('+')[0]

        return datetime.fromisoformat(dt_str)
