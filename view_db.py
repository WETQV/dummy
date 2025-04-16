"""
Утилита для просмотра данных в SQLite базе данных.

Этот скрипт позволяет просматривать структуру и содержимое базы данных,
которая была создана скриптом processor.py.

Возможности:
1. Просмотр списка всех таблиц в БД
2. Просмотр структуры каждой таблицы (колонки и их типы)
3. Просмотр примеров данных из таблиц (первые 10 записей)
4. Форматирование сложных данных (JSON) для удобочитаемости
"""

import sqlite3
import json
import argparse
import os
from typing import Optional, Dict, Any


def format_value(key: str, value: Any) -> str:
    """
    Форматирует значение для вывода на экран.
    
    Аргументы:
        key: Имя колонки
        value: Значение для форматирования
        
    Возвращает:
        str: Отформатированное значение
    """
    if value is None:
        return "NULL"
        
    # Форматируем JSON поля
    if key in ['images', 'tags', 'hair', 'address', 'bank', 'company'] and value:
        try:
            data = json.loads(value)
            # Разные типы данных форматируем по-разному
            if isinstance(data, list):
                if key == 'images' and len(data) > 0:
                    # Для изображений показываем только первую ссылку и количество
                    first_image = data[0]
                    if len(data) > 1:
                        return f"{first_image} (+ еще {len(data)-1})"
                    return first_image
                elif key == 'tags' and len(data) > 0:
                    # Для тегов показываем список
                    return ", ".join(data)
                return str(data)
            elif isinstance(data, dict) and data:
                # Сокращённый вывод для словарей
                items = [f"{k}: {v}" for k, v in list(data.items())[:3]]
                if len(data) > 3:
                    items.append(f"... ({len(data)-3} скрыто)")
                return "{" + ", ".join(items) + "}"
            return str(data)
        except (json.JSONDecodeError, TypeError):
            return str(value)
            
    # Длинные текстовые поля сокращаем
    if isinstance(value, str):
        if key in ['description', 'body'] and len(value) > 50:
            return f"{value[:47]}..."
            
    # Форматируем числовые значения
    if isinstance(value, (int, float)):
        if key in ['price', 'discountPercentage']:
            return f"{value:.2f}"
            
    return str(value)


def get_table_info(cursor: sqlite3.Cursor, table_name: str) -> Dict[str, Any]:
    """
    Получает информацию о таблице (количество строк, структура).
    
    Аргументы:
        cursor: Курсор базы данных
        table_name: Имя таблицы
        
    Возвращает:
        dict: Информация о таблице
    """
    # Получаем структуру таблицы
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    
    # Получаем количество строк в таблице
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    
    return {
        "name": table_name,
        "columns": columns,
        "row_count": row_count
    }


def print_table_data(cursor: sqlite3.Cursor, table_name: str, limit: int = 10) -> None:
    """
    Выводит данные из указанной таблицы.
    
    Аргументы:
        cursor: Курсор базы данных
        table_name: Имя таблицы
        limit: Максимальное количество строк для вывода
    """
    # Получаем структуру таблицы
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    
    print("\nСтруктура таблицы:")
    for col in columns:
        is_pk = " [PRIMARY KEY]" if col['pk'] else ""
        not_null = " [NOT NULL]" if col['notnull'] else ""
        print(f"  {col['name']} ({col['type']}{is_pk}{not_null})")
    
    # Получаем данные из таблицы
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cursor.fetchall()
    
    if not rows:
        print("\n[ИНФОРМАЦИЯ] Таблица пуста.")
        return
        
    print(f"\nДанные: (первые {min(limit, len(rows))} строк)")
    
    for i, row in enumerate(rows, 1):
        row_dict = dict(row)
        print(f"\n--- Запись #{i} ---")
        
        # Выводим значения с форматированием
        for key, value in row_dict.items():
            formatted_value = format_value(key, value)
            print(f"  {key}: {formatted_value}")


def view_database(db_name: str, table_name: Optional[str] = None) -> None:
    """
    Основная функция для просмотра данных в базе данных.
    
    Аргументы:
        db_name: Путь к файлу базы данных
        table_name: Имя конкретной таблицы для просмотра (опционально)
    """
    if not os.path.exists(db_name):
        print(f"[ОШИБКА] База данных '{db_name}' не найдена.")
        print(f"Сначала запустите скрипт processor.py для создания БД:")
        print(f"python dummy/processor.py")
        print(f"или с флагом очистки:")
        print(f"python dummy/processor.py --clean")
        return
        
    try:
        # Подключение к базе данных
        conn = sqlite3.connect(db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Получаем список всех таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print(f"[ИНФОРМАЦИЯ] База данных '{db_name}' не содержит таблиц.")
            return
            
        # Если указана конкретная таблица, выводим только её
        if table_name:
            # Проверяем существование таблицы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                print(f"[ОШИБКА] Таблица '{table_name}' не найдена в БД.")
                print(f"Доступные таблицы: {', '.join(t[0] for t in tables)}")
                return
                
            print(f"=== Таблица: {table_name} ===")
            print_table_data(cursor, table_name)
            
            # Получаем количество строк в таблице
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nВсего записей: {count}")
            
        # Иначе выводим все таблицы
        else:
            print(f"Таблицы в базе данных {db_name}:")
            
            # Собираем информацию по таблицам
            table_infos = []
            for table in tables:
                table_name = table[0]
                table_infos.append(get_table_info(cursor, table_name))
            
            # Сортируем таблицы по количеству строк (по убыванию)
            table_infos.sort(key=lambda x: x["row_count"], reverse=True)
            
            # Выводим информацию по каждой таблице
            for table_info in table_infos:
                table_name = table_info["name"]
                row_count = table_info["row_count"]
                
                print(f"\n{'='*50}")
                print(f"=== Таблица: {table_name} ({row_count} записей) ===")
                print(f"{'='*50}")
                
                print_table_data(cursor, table_name)
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"[ОШИБКА] Ошибка при работе с базой данных: {e}")


if __name__ == "__main__":
    # Разбор аргументов командной строки
    parser = argparse.ArgumentParser(description="Просмотр данных в SQLite базе данных")
    parser.add_argument("--db", default="dummy_data.db", help="Путь к файлу базы данных")
    parser.add_argument("--table", help="Имя конкретной таблицы для просмотра")
    args = parser.parse_args()
    
    # Запуск просмотра базы данных
    view_database(args.db, args.table) 