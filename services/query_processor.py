import re
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class QueryProcessor:
    def __init__(self, db, llm_handler):
        self.db = db
        self.llm_handler = llm_handler

        # Названия месяцев
        self.months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
            'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
            'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
        }

    def _extract_date_range(self, text: str) -> Optional[Tuple[datetime, datetime]]:
        """Извлечение диапазона дат из текста запроса"""
        try:
            text_lower = text.lower()

            # Обработка дат (сегодня, вчера, неделя, месяц)
            relative_dates = self._extract_relative_dates(text_lower)
            if relative_dates:
                return relative_dates

            # Обработка точных дат
            exact_dates = self._extract_exact_dates(text_lower)
            if exact_dates:
                return exact_dates

            # Обработка диапазонов дат
            date_range = self._extract_date_range_patterns(text_lower)
            if date_range:
                return date_range

            # Обработка периодов (за последние дни/недели/месяцы)
            period = self._extract_period(text_lower)
            if period:
                return period

            return None

        except Exception as e:
            logger.error(f"Ошибка при извлечении дат: {e}")
            return None

    def _extract_relative_dates(self, text: str) -> Optional[Tuple[datetime, datetime]]:
        """Обработка относительных временных периодов"""
        today = datetime.now()

        if 'сегодня' in text:
            start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = today.replace(hour=23, minute=59, second=59, microsecond=999999)
            return start_date, end_date

        elif 'вчера' in text:
            yesterday = today - timedelta(days=1)
            start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            return start_date, end_date

        elif 'неделя' in text or 'неделю' in text:
            if 'последняя неделя' in text or 'прошлая неделя' in text:
                # Прошлая неделя (полная)
                end_of_last_week = today - timedelta(days=today.weekday() + 1)
                start_of_last_week = end_of_last_week - timedelta(days=6)
                start_date = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = end_of_last_week.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                # Текущая неделя
                start_of_week = today - timedelta(days=today.weekday())
                end_of_week = start_of_week + timedelta(days=6)
                start_date = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999)
            return start_date, end_date

        elif 'месяц' in text:
            if 'последний месяц' in text or 'прошлый месяц' in text:
                # Прошлый месяц
                first_day_of_current_month = today.replace(day=1)
                last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
                first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
                start_date = first_day_of_previous_month.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = last_day_of_previous_month.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                # Текущий месяц
                start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(hour=23, minute=59, second=59, microsecond=999999)
            return start_date, end_date

        elif 'квартал' in text:
            quarter = self._extract_quarter(text, today)
            if quarter:
                return quarter

        return None

    def _extract_exact_dates(self, text: str) -> Optional[Tuple[datetime, datetime]]:
        """Обработка точных дат"""
        # Паттерн для русских дат: 15 января 2024
        ru_pattern = r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})'
        ru_matches = list(re.finditer(ru_pattern, text, re.IGNORECASE))

        # Паттерн для числовых дат: 15.01.2024, 15/01/2024, 15-01-2024
        num_pattern = r'(\d{1,2})[./-](\d{1,2})[./-](\d{4})'
        num_matches = list(re.finditer(num_pattern, text))

        all_matches = ru_matches + num_matches

        if not all_matches:
            return None

        # Если найдена только одна дата: используем ее как и начало, и конец дня
        if len(all_matches) == 1:
            match = all_matches[0]
            if match in ru_matches:
                day, month_name, year = match.groups()
                month = self.months.get(month_name.lower())
            else:
                day, month, year = match.groups()
                month = int(month)

            if month:
                date = datetime(int(year), month, int(day))
                start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = date.replace(hour=23, minute=59, second=59, microsecond=999999)
                return start_date, end_date

        # Если найдено несколько дат, берем первую и последнюю
        elif len(all_matches) >= 2:
            # Сортируем по позиции в тексте
            all_matches.sort(key=lambda x: x.start())

            first_match = all_matches[0]
            last_match = all_matches[-1]

            # Обрабатываем первую дату
            if first_match in ru_matches:
                day, month_name, year = first_match.groups()
                month = self.months.get(month_name.lower())
                if month:
                    start_date = datetime(int(year), month, int(day), 0, 0, 0)
            else:
                day, month, year = first_match.groups()
                start_date = datetime(int(year), int(month), int(day), 0, 0, 0)

            # Обрабатываем последнюю дату
            if last_match in ru_matches:
                day, month_name, year = last_match.groups()
                month = self.months.get(month_name.lower())
                if month:
                    end_date = datetime(int(year), month, int(day), 23, 59, 59, 999999)
            else:
                day, month, year = last_match.groups()
                end_date = datetime(int(year), int(month), int(day), 23, 59, 59, 999999)

            return start_date, end_date

        return None

    def _extract_date_range_patterns(self, text: str) -> Optional[Tuple[datetime, datetime]]:
        """Обработка диапазонов дат с указанием 'с ... по ...', 'от ... до ...'"""
        patterns = [
            # С 15 января 2024 по 20 января 2024
            r'с\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\s+по\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})',
            # С 15.01.2024 по 20.01.2024
            r'с\s+(\d{1,2})[./-](\d{1,2})[./-](\d{4})\s+по\s+(\d{1,2})[./-](\d{1,2})[./-](\d{4})',
            # От 15 января 2024 до 20 января 2024
            r'от\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\s+до\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 6:
                    # Формат с русскими месяцами
                    day1, month_name1, year1, day2, month_name2, year2 = groups
                    month1 = self.months.get(month_name1.lower())
                    month2 = self.months.get(month_name2.lower())
                    if month1 and month2:
                        start_date = datetime(int(year1), month1, int(day1), 0, 0, 0)
                        end_date = datetime(int(year2), month2, int(day2), 23, 59, 59, 999999)
                        return start_date, end_date
                elif len(groups) == 6:
                    # Формат с числовыми датами
                    day1, month1, year1, day2, month2, year2 = groups
                    start_date = datetime(int(year1), int(month1), int(day1), 0, 0, 0)
                    end_date = datetime(int(year2), int(month2), int(day2), 23, 59, 59, 999999)
                    return start_date, end_date

        return None

    def _extract_period(self, text: str) -> Optional[Tuple[datetime, datetime]]:
        """Обработка периодов: 'за последние 7 дней', 'за прошлый месяц' и т.д."""
        today = datetime.now()

        # Паттерны для периодов
        patterns = [
            (r'последние\s+(\d+)\s+дн(?:я|ей)', 'days'),
            (r'последние\s+(\d+)\s+недел(?:я|и|ь)', 'weeks'),
            (r'последние\s+(\d+)\s+месяц(?:а|ев)', 'months'),
            (r'за\s+(\d+)\s+дн(?:я|ей)', 'days'),
            (r'за\s+(\d+)\s+недел(?:я|и|ь)', 'weeks'),
            (r'за\s+(\d+)\s+месяц(?:а|ев)', 'months'),
        ]

        for pattern, unit in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = int(match.group(1))

                if unit == 'days':
                    start_date = today - timedelta(days=value)
                elif unit == 'weeks':
                    start_date = today - timedelta(weeks=value)
                elif unit == 'months':
                    # расчет месяцев
                    start_date = today - timedelta(days=value * 30)

                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(hour=23, minute=59, second=59, microsecond=999999)
                return start_date, end_date

        return None

    def _extract_quarter(self, text: str, reference_date: datetime) -> Optional[Tuple[datetime, datetime]]:
        """Обработка кварталов"""
        # Определение текущего квартала
        current_quarter = (reference_date.month - 1) // 3 + 1
        current_year = reference_date.year

        # Проверяем указание конкретного квартала
        quarter_pattern = r'(\d+)(?:-?й|\s)?\s*квартал(?:\s+(\d{4}))?'
        match = re.search(quarter_pattern, text, re.IGNORECASE)

        if match:
            quarter_num = int(match.group(1))
            year = int(match.group(2)) if match.group(2) else current_year

            if 1 <= quarter_num <= 4:
                start_month = (quarter_num - 1) * 3 + 1
                end_month = start_month + 2

                # Последний день месяца
                if end_month == 12:
                    end_date = datetime(year, 12, 31)
                else:
                    next_month = datetime(year, end_month + 1, 1)
                    end_date = next_month - timedelta(days=1)

                start_date = datetime(year, start_month, 1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                return start_date, end_date

        # Если указан просто "квартал", возвращаем текущий квартал
        elif 'квартал' in text and not re.search(r'\d+.*квартал', text):
            return self._get_quarter_dates(current_year, current_quarter)

        return None

    def _get_quarter_dates(self, year: int, quarter: int) -> Tuple[datetime, datetime]:
        """Получение дат начала и конца квартала"""
        start_month = (quarter - 1) * 3 + 1
        end_month = start_month + 2

        # Последний день месяца
        if end_month == 12:
            end_date = datetime(year, 12, 31)
        else:
            next_month = datetime(year, end_month + 1, 1)
            end_date = next_month - timedelta(days=1)

        start_date = datetime(year, start_month, 1)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        return start_date, end_date

    async def process_query(self, question: str) -> str:
        """Основной метод обработки запроса"""
        try:
            # Извлечение диапазона дат из вопроса
            date_range = self._extract_date_range(question)

            # Подготовка контекста для LLM
            context = {
                "question": question,
                "date_range": date_range,
                "date_range_str": self._format_date_range(date_range) if date_range else None
            }

            # Генерация SQL через LLM с учетом дат
            sql_query = await self.llm_handler.generate_sql_query(context)

            # Выполнение SQL запроса
            result = await self.db.execute_scalar(sql_query)

            # Форматирование результата
            if result is None:
                return "0"

            if isinstance(result, float):
                if result.is_integer():
                    return str(int(result))
                return str(round(result, 2))

            return str(result)

        except Exception as e:
            logger.error(f"Ошибка при обработке запроса: {e}")
            return f"Ошибка при обработке запроса: {str(e)}"

    def _format_date_range(self, date_range: Tuple[datetime, datetime]) -> str:
        """Форматирование диапазона дат для SQL"""
        if not date_range:
            return ""

        start_date, end_date = date_range
        return f"BETWEEN '{start_date.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'"

