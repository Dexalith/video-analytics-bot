import asyncio
import logging

from db.database import Database
from db.config import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def load_videos_data():
    """Загрузка данных о видео из JSON файла"""
    try:
        db = Database(settings.DATABASE_URL)
        await db.connect()

        await db.load_json_data('videos.json')
        logger.info("Данные успешно загружены в базу данных")

    except FileNotFoundError:
        logger.info("Файл videos.json не найден!")
    except Exception as e:
        logger.info(f"Ошибка: {e}")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(load_videos_data())