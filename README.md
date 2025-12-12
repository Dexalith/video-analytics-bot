# Telegram Bot для аналитики видео

Бот для анализа статистики видео на основе естественного языка.

## Особенности
- Принимает запросы на естественном языке (русский)
- Использует LLM для преобразования запросов в SQL
- Поддерживает PostgreSQL
- Работает с двумя таблицами: видео и почасовые снапшоты
- Возвращает числовые ответы

## Требования
- Docker и Docker Compose
- API ключ OpenAI (или другой LLM провайдер)
- Токен Telegram бота

## Быстрый запуск

1. Клонируйте репозиторий
2. Скачайте JSON файл с данными в корень проекта
3. Создание таблиц в БД(pPostgreSQL):
```psql
psql -U your_user -d video_analytics -f init_db.sql
```

4. Настройте переменные окружения:
```bash

python -m venv vevn
source venv/bin/activate

pip install -r requirements.txt

python -m services.load_data # из корня проекта

python -m bot # запуск бота
```

```bash

cp .env.example .env
# Отредактируйте .env файл:
# TELEGRAM_BOT_TOKEN=ваш_токен TG
# GROQ_API_KEY=ваш_api_ключ
# DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_analytics
# Для работы Groq требуется VPN(Перед запуском бота включите VPN)
```