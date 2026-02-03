import asyncio
import logging
import sys
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорты aiogram 2.x
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage

# Наши модули
from config import bot_config, db_config, llm_config
from database.connection import db
from services.llm_service import LLMService

# Проверяем токен бота
if not bot_config.token:
    logger.error("❌ BOT_TOKEN не установлен в .env файле!")
    logger.error("Получите токен у @BotFather в Telegram")
    sys.exit(1)

# Инициализация бота
bot = Bot(token=bot_config.token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
llm_service = LLMService()

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    welcome_text = """
🤖 <b>Бот аналитики видео</b>

Я умею отвечать на вопросы на естественном языке о видео и их статистике.

Примеры вопросов:
• Сколько всего видео в системе?
• Сколько видео у креатора с id 123?
• Сколько видео набрало больше 100000 просмотров?
• На сколько просмотров выросли все видео 28 ноября 2025?
• Сколько разных видео получали новые просмотры 27 ноября 2025?

Просто напишите вопрос, и я верну число!
    """
    await message.answer(welcome_text, parse_mode='HTML')

@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    """Обработчик команды /help"""
    help_text = """
📚 <b>Помощь</b>

Формат вопросов:
• Количество: "Сколько видео...?"
• Сумма: "Сумма просмотров...", "Всего лайков..."
• Прирост: "На сколько выросло...", "Прирост комментариев..."
• Даты: "28 ноября 2025", "с 1 по 5 ноября 2025"

Примеры:
• Сколько всего видео есть в системе?
• Сколько видео у креатора с id 123 вышло с 1 по 5 ноября 2025?
• Сколько видео набрало больше 100000 просмотров?
• На сколько просмотров в сумме выросли все видео 28 ноября 2025?
• Сколько разных видео получали новые просмотры 27 ноября 2025?
    """
    await message.answer(help_text, parse_mode='HTML')

@dp.message_handler(commands=['stats'])
async def cmd_stats(message: types.Message):
    """Статистика базы данных"""
    try:
        # Количество видео
        video_count = await db.execute_query("SELECT COUNT(*) FROM videos;")
        
        # Количество снапшотов
        snapshot_count = await db.execute_query("SELECT COUNT(*) FROM video_snapshots;")
        
        # Количество креаторов
        creator_count = await db.execute_query("SELECT COUNT(DISTINCT creator_id) FROM videos;")
        
        stats_text = f"""
📊 <b>Статистика базы данных:</b>

• Видео: {video_count:,}
• Почасовых снапшотов: {snapshot_count:,}
• Уникальных креаторов: {creator_count:,}

База данных готова к работе!
        """.replace(",", " ")
        
        await message.answer(stats_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await message.answer(f"❌ Ошибка получения статистики: {str(e)}")

@dp.message_handler()
async def handle_user_query(message: types.Message):
    """Обработчик текстовых запросов"""
    user_query = message.text.strip()
    
    if not user_query:
        await message.answer("Пожалуйста, введите вопрос.")
        return
    
    # Отправляем сообщение о обработке
    processing_msg = await message.answer("⏳ Обрабатываю запрос...")
    
    try:
        # Генерируем SQL запрос через LLM
        sql_query, explanation = await llm_service.generate_sql_from_text(user_query)
        logger.info(f"SQL запрос: {sql_query}")
        logger.info(f"Объяснение: {explanation}")
        
        # Выполняем запрос к БД
        result = await db.execute_query(sql_query)
        
        if result is None:
            await processing_msg.edit_text(
                "❌ Не удалось получить данные. Проверьте формулировку запроса."
            )
            return
        
        # Форматируем результат
        if isinstance(result, (int, float)):
            formatted = f"{result:,}".replace(",", " ")
        else:
            formatted = str(result)
        
        # Отправляем ответ
        await processing_msg.edit_text(
            f"📊 <b>Ответ:</b> {formatted}\n\n"
            f"<i>Ваш запрос:</i> {user_query}",
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}", exc_info=True)
        await processing_msg.edit_text(
            f"❌ Произошла ошибка:\n{str(e)}\n\n"
            "Попробуйте переформулировать вопрос или использовать более простой запрос.",
            parse_mode='HTML'
        )

async def on_startup(dp):
    """Действия при запуске бота"""
    logger.info("Подключение к базе данных...")
    await db.connect()
    await db.create_tables()
    
    # Проверяем подключение
    try:
        result = await db.execute_query("SELECT 1;")
        logger.info(f"✅ Подключение к БД успешно: {result}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        logger.error("Проверьте настройки PostgreSQL в .env файле")
    
    # Проверяем OpenAI API
    if llm_config.openai_api_key:
        logger.info(f"✅ OpenAI API ключ настроен (модель: {llm_config.openai_model})")
    else:
        logger.warning("⚠️ OpenAI API ключ не найден, будет использован простой парсер")
    
    logger.info("Бот запущен и готов к работе!")
    logger.info(f"Имя бота: @{(await bot.get_me()).username}")

async def on_shutdown(dp):
    """Действия при выключении бота"""
    logger.info("Остановка бота...")
    await db.close()

if __name__ == '__main__':
    # Запускаем бота
    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup,
        on_shutdown=on_shutdown
    )