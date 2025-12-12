import asyncio
import logging
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

from db.config import settings
from db.database import Database
from services.lm_handler import LLMHandler
from services.query_processor import QueryProcessor


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()

db: Database = None
query_processor: QueryProcessor = None


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    welcome_text = """
    👋 Привет! Я бот для аналитики видео-контента.

    Я могу отвечать на вопросы на естественном языке, например:
    • Сколько всего видео есть в системе?
    • Сколько видео у креатора с id ... вышло с 1 по 5 ноября 2025?
    • Сколько видео набрало больше 100000 просмотров?
    • На сколько просмотров выросли все видео 28 ноября 2025?
    • Сколько разных видео получали новые просмотры 27 ноября 2025?

    Просто задайте вопрос в свободной форме на русском языке!
    """
    await message.answer(welcome_text)


@router.message()
async def handle_message(message: Message):
    """Обработчик текстовых сообщений"""
    try:
        # Показываем статус обработки
        processing_msg = await message.answer("🔄 Обрабатываю запрос...")

        # Обработка запроса
        result = await query_processor.process_query(message.text)

        # Удаляем сообщение о обработке
        await processing_msg.delete()

        # Отправляем результат
        await message.answer(f"📊 Результат: {result}")

    except Exception as e:
        logger.error(f"Error handling message: {e}")
        await message.answer(f"Произошла ошибка при обработке запроса: {str(e)}")


async def main():
    """Основная функция запуска бота"""
    global db, query_processor

    # Инициализация бота
    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    dp.include_router(router)

    # Инициализация базы данных
    db = Database(settings.DATABASE_URL)
    await db.connect()

    # Инициализация LLM
    llm_handler = LLMHandler()

    # Инициализация процессора запросов
    query_processor = QueryProcessor(db, llm_handler)

    logger.info("Bot is starting...")

    try:
        await dp.start_polling(bot)
    finally:
        await db.disconnect()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
