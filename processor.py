"""
Скрипт для работы с API DummyJSON и сохранения данных в SQLite базу данных.

Этот скрипт выполняет следующие задачи:
1. Получает данные с публичного API DummyJSON (https://dummyjson.com/)
2. Обрабатывает полученные данные
3. Сохраняет их в локальную SQLite базу данных
4. Логирует весь процесс для дальнейшей отладки
"""

import requests
import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
import os
import sys

# ========================
# === НАСТРОЙКИ СКРИПТА ===
# ========================

# API и база данных
BASE_API_URL = "https://dummyjson.com/"
DB_NAME = "dummy_data.db"

# Настройка логирования
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger('dummy_processor')

# Лимиты запросов по умолчанию
DEFAULT_LIMIT = 10
DEFAULT_SKIP = 0
DEFAULT_SEARCH = "iPhone"  # Поиск по умолчанию для продуктов


# =============================
# === ФУНКЦИИ РАБОТЫ С API ===
# =============================

def fetch_api_data(entity: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Получает данные с API DummyJSON для указанной сущности.
    
    Аргументы:
        entity (str): Тип данных для запроса (напр., 'products', 'users', 'posts').
        params (dict, optional): Параметры GET-запроса (limit, skip, q и т.д.).
    
    Возвращает:
        dict или None: Данные из API в виде словаря или None в случае ошибки.
    """
    url = f"{BASE_API_URL}{entity}"
    logger.info(f"Запрос к API: {url} с параметрами: {params}")
    
    try:
        # Выполняем запрос к API с таймаутом 10 секунд
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Выбрасывает исключение при HTTP-ошибках (4xx, 5xx)
        
        logger.info(f"API ответил успешно: HTTP {response.status_code}")
        return response.json()
        
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут при запросе к {url} - API не отвечает")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сетевого запроса к API {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Получен некорректный JSON от {url}: {e}")
        return None


# ================================
# === ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ ===
# ================================

def get_db_connection() -> Optional[sqlite3.Connection]:
    """
    Устанавливает соединение с базой данных SQLite.
    
    Возвращает:
        sqlite3.Connection или None: Объект соединения с БД или None при ошибке
    """
    try:
        # Проверяем, существует ли директория для БД
        db_dir = os.path.dirname(DB_NAME)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logger.info(f"Создана директория для БД: {db_dir}")
        
        # Подключаемся к БД
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row  # Результаты запросов как словари
        logger.info(f"Успешное подключение к базе данных: {DB_NAME}")
        return conn
        
    except sqlite3.Error as e:
        logger.error(f"Ошибка подключения к базе данных {DB_NAME}: {e}")
        return None
    except OSError as e:
        logger.error(f"Ошибка операции с файловой системой: {e}")
        return None


def init_db_table(conn: sqlite3.Connection, table_name: str, schema_sql: str) -> bool:
    """
    Инициализирует (создает) таблицу в БД, если она не существует.
    
    Аргументы:
        conn: Активное соединение с базой данных
        table_name: Имя создаваемой таблицы
        schema_sql: SQL-запрос для создания таблицы (CREATE TABLE...)
        
    Возвращает:
        bool: True если таблица создана или уже существует, False при ошибке
    """
    try:
        cursor = conn.cursor()
        
        # Проверяем существование таблицы
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        
        if cursor.fetchone() is None:
            logger.info(f"Таблица '{table_name}' не найдена. Создание...")
            cursor.execute(schema_sql)
            conn.commit()
            logger.info(f"[УСПЕХ] Таблица '{table_name}' успешно создана")
        else:
            logger.info(f"Таблица '{table_name}' уже существует")
            
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации таблицы '{table_name}': {e}")
        conn.rollback()  # Откатываем изменения при ошибке
        return False


def save_data_to_db(conn: sqlite3.Connection, table_name: str, data: List[Tuple], columns: List[str]) -> int:
    """
    Сохраняет данные в указанную таблицу базы данных.
    
    Аргументы:
        conn: Активное соединение с БД
        table_name: Имя таблицы для вставки данных
        data: Список кортежей с данными
        columns: Список имен колонок в том же порядке, что и данные в кортежах
        
    Возвращает:
        int: Количество успешно сохраненных записей
    """
    if not data:
        logger.info("Нет данных для сохранения в базу данных")
        return 0

    # Формируем SQL-запрос с нужным количеством параметров
    placeholders = ', '.join(['?'] * len(columns))
    cols_string = ', '.join(columns)
    
    # Используем INSERT OR IGNORE для пропуска дубликатов по PRIMARY KEY
    sql = f"INSERT OR IGNORE INTO {table_name} ({cols_string}) VALUES ({placeholders})"

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, data)
        conn.commit()
        
        saved_count = cursor.rowcount  # Количество новых вставленных строк
        
        if saved_count > 0:
            logger.info(f"[УСПЕХ] Сохранено {saved_count} новых записей в таблицу '{table_name}'")
        else:
            logger.info(f"Новые записи не были добавлены в таблицу '{table_name}' (возможно, уже существуют)")
            
        return saved_count
        
    except sqlite3.Error as e:
        logger.error(f"Ошибка при сохранении данных в таблицу '{table_name}': {e}")
        conn.rollback()  # Откатываем транзакцию при ошибке
        return 0


# =======================================
# === ФУНКЦИИ ОБРАБОТКИ ТИПОВ ДАННЫХ ===
# =======================================

def process_products(search_query: str = DEFAULT_SEARCH):
    """
    Получает и сохраняет данные о продуктах из API DummyJSON.
    
    Аргументы:
        search_query: Поисковый запрос для фильтрации продуктов
    """
    logger.info(f"[НАЧАЛО] Обработка продуктов по запросу: '{search_query}'")

    # 1. Получаем данные с API
    api_response = fetch_api_data("products/search", params={"q": search_query})

    # Проверяем успешность запроса и наличие данных
    if not api_response or 'products' not in api_response or not api_response['products']:
        logger.warning(f"[ПРЕДУПРЕЖДЕНИЕ] Не удалось получить данные о продуктах или продукты по запросу '{search_query}' не найдены")
        return

    products_list = api_response['products']
    logger.info(f"Найдено {len(products_list)} продуктов")

    # 2. Подключаемся к БД
    conn = get_db_connection()
    if not conn:
        logger.error("[ОШИБКА] Не удалось подключиться к базе данных")
        return

    try:
        # 3. Создаём таблицу, если её нет
        table_name = "products"
        products_schema = """
        CREATE TABLE products (
            api_id INTEGER PRIMARY KEY NOT NULL,
            title TEXT,
            description TEXT,
            price REAL,
            discountPercentage REAL,
            rating REAL,
            stock INTEGER,
            brand TEXT,
            category TEXT,
            thumbnail TEXT,
            images TEXT  -- JSON-строка со списком URL изображений
        );
        """
        products_columns = [
            'api_id', 'title', 'description', 'price', 'discountPercentage',
            'rating', 'stock', 'brand', 'category', 'thumbnail', 'images'
        ]

        if not init_db_table(conn, table_name, products_schema):
            logger.error("[ОШИБКА] Не удалось инициализировать таблицу products")
            return

        # 4. Подготовка данных для сохранения
        data_to_save = []
        for product in products_list:
            # Проверяем наличие обязательного поля ID
            if 'id' not in product:
                logger.warning(f"[ПРЕДУПРЕЖДЕНИЕ] Продукт без ID пропущен: {product.get('title', 'Неизвестно')}")
                continue

            # Преобразуем данные продукта в кортеж для вставки в БД
            product_tuple = (
                product.get('id'),                      # api_id
                product.get('title'),                   # title
                product.get('description'),             # description
                product.get('price'),                   # price
                product.get('discountPercentage'),      # discountPercentage
                product.get('rating'),                  # rating
                product.get('stock'),                   # stock
                product.get('brand'),                   # brand
                product.get('category'),                # category
                product.get('thumbnail'),               # thumbnail
                json.dumps(product.get('images', []))   # images (как JSON-строка)
            )
            data_to_save.append(product_tuple)

        # 5. Сохраняем данные в БД
        saved_count = save_data_to_db(conn, table_name, data_to_save, products_columns)
        
        # 6. Выводим итог
        total_count = len(products_list)
        if saved_count > 0:
            logger.info(f"[ЗАВЕРШЕНО] Обработка продуктов успешно завершена. "
                      f"Добавлено {saved_count} из {total_count} записей.")
        else:
            logger.info(f"[ИНФОРМАЦИЯ] Обработка продуктов завершена. Новые записи не добавлены.")

    finally:
        # 7. Закрываем соединение с БД
        conn.close()
        logger.info("Соединение с базой данных закрыто")


def process_users(limit: int = DEFAULT_LIMIT, skip: int = DEFAULT_SKIP):
    """
    Получает и сохраняет данные о пользователях из API DummyJSON.
    
    Аргументы:
        limit: Максимальное количество пользователей для получения
        skip: Количество пользователей для пропуска (для пагинации)
    """
    logger.info(f"[НАЧАЛО] Обработка пользователей (limit: {limit}, skip: {skip})")

    # 1. Получаем данные с API
    api_response = fetch_api_data("users", params={"limit": limit, "skip": skip})

    # Проверяем успешность запроса и наличие данных
    if not api_response or 'users' not in api_response or not api_response['users']:
        logger.warning("[ПРЕДУПРЕЖДЕНИЕ] Не удалось получить данные о пользователях или пользователи не найдены")
        return

    users_list = api_response['users']
    logger.info(f"Получено {len(users_list)} пользователей")

    # 2. Подключаемся к БД
    conn = get_db_connection()
    if not conn:
        logger.error("[ОШИБКА] Не удалось подключиться к базе данных")
        return

    try:
        # 3. Создаём таблицу, если её нет
        table_name = "users"
        users_schema = """
        CREATE TABLE users (
            api_id INTEGER PRIMARY KEY NOT NULL,
            firstName TEXT,
            lastName TEXT,
            maidenName TEXT,
            age INTEGER,
            gender TEXT,
            email TEXT,
            phone TEXT,
            username TEXT,
            password TEXT,
            birthDate TEXT,
            image TEXT,
            bloodGroup TEXT,
            height REAL,
            weight REAL,
            eyeColor TEXT,
            hair TEXT,           -- JSON-строка
            domain TEXT,
            ip TEXT,
            address TEXT,        -- JSON-строка
            macAddress TEXT,
            university TEXT,
            bank TEXT,           -- JSON-строка
            company TEXT,        -- JSON-строка
            ein TEXT,
            ssn TEXT,
            userAgent TEXT
        );
        """
        users_columns = [
            'api_id', 'firstName', 'lastName', 'maidenName', 'age', 'gender', 'email', 
            'phone', 'username', 'password', 'birthDate', 'image', 'bloodGroup',
            'height', 'weight', 'eyeColor', 'hair', 'domain', 'ip', 'address',
            'macAddress', 'university', 'bank', 'company', 'ein', 'ssn', 'userAgent'
        ]

        if not init_db_table(conn, table_name, users_schema):
            logger.error("[ОШИБКА] Не удалось инициализировать таблицу users")
            return

        # 4. Подготовка данных для сохранения
        data_to_save = []
        for user in users_list:
            # Проверяем наличие обязательного поля ID
            if 'id' not in user:
                logger.warning(f"[ПРЕДУПРЕЖДЕНИЕ] Пользователь без ID пропущен: {user.get('username', 'Неизвестно')}")
                continue

            # Конвертируем сложные объекты в JSON-строки
            hair_json = json.dumps(user.get('hair', {}))
            address_json = json.dumps(user.get('address', {}))
            bank_json = json.dumps(user.get('bank', {}))
            company_json = json.dumps(user.get('company', {}))

            # Преобразуем данные пользователя в кортеж для вставки в БД
            user_tuple = (
                user.get('id'),           # api_id
                user.get('firstName'),    # firstName
                user.get('lastName'),     # lastName
                user.get('maidenName'),   # maidenName
                user.get('age'),          # age
                user.get('gender'),       # gender
                user.get('email'),        # email
                user.get('phone'),        # phone
                user.get('username'),     # username
                user.get('password'),     # password
                user.get('birthDate'),    # birthDate
                user.get('image'),        # image
                user.get('bloodGroup'),   # bloodGroup
                user.get('height'),       # height
                user.get('weight'),       # weight
                user.get('eyeColor'),     # eyeColor
                hair_json,                # hair (JSON-строка)
                user.get('domain'),       # domain
                user.get('ip'),           # ip
                address_json,             # address (JSON-строка)
                user.get('macAddress'),   # macAddress
                user.get('university'),   # university
                bank_json,                # bank (JSON-строка)
                company_json,             # company (JSON-строка)
                user.get('ein'),          # ein
                user.get('ssn'),          # ssn
                user.get('userAgent')     # userAgent
            )
            data_to_save.append(user_tuple)

        # 5. Сохраняем данные в БД
        saved_count = save_data_to_db(conn, table_name, data_to_save, users_columns)
        
        # 6. Выводим итог
        total_count = len(users_list)
        if saved_count > 0:
            logger.info(f"[ЗАВЕРШЕНО] Обработка пользователей успешно завершена. "
                      f"Добавлено {saved_count} из {total_count} записей.")
        else:
            logger.info(f"[ИНФОРМАЦИЯ] Обработка пользователей завершена. Новые записи не добавлены.")

    finally:
        # 7. Закрываем соединение с БД
        conn.close()
        logger.info("Соединение с базой данных закрыто")


def process_posts(limit: int = DEFAULT_LIMIT, skip: int = DEFAULT_SKIP, user_id: Optional[int] = None):
    """
    Получает и сохраняет данные о постах из API DummyJSON.
    
    Аргументы:
        limit: Максимальное количество постов для получения
        skip: Количество постов для пропуска (для пагинации)
        user_id: ID пользователя, если нужны только его посты (опционально)
    """
    if user_id:
        logger.info(f"[НАЧАЛО] Обработка постов пользователя #{user_id} (limit: {limit}, skip: {skip})")
        endpoint = f"users/{user_id}/posts"
    else:
        logger.info(f"[НАЧАЛО] Обработка всех постов (limit: {limit}, skip: {skip})")
        endpoint = "posts"

    # 1. Получаем данные с API
    api_response = fetch_api_data(endpoint, params={"limit": limit, "skip": skip})

    # Проверяем успешность запроса и наличие данных
    if not api_response or 'posts' not in api_response or not api_response['posts']:
        logger.warning("[ПРЕДУПРЕЖДЕНИЕ] Не удалось получить данные о постах или посты не найдены")
        return

    posts_list = api_response['posts']
    logger.info(f"Получено {len(posts_list)} постов")

    # 2. Подключаемся к БД
    conn = get_db_connection()
    if not conn:
        logger.error("[ОШИБКА] Не удалось подключиться к базе данных")
        return

    try:
        # 3. Создаём таблицу, если её нет
        table_name = "posts"
        posts_schema = """
        CREATE TABLE posts (
            api_id INTEGER PRIMARY KEY NOT NULL,
            title TEXT,
            body TEXT,
            userId INTEGER,
            tags TEXT,        -- JSON-строка со списком тегов
            reactions INTEGER
        );
        """
        posts_columns = ['api_id', 'title', 'body', 'userId', 'tags', 'reactions']

        if not init_db_table(conn, table_name, posts_schema):
            logger.error("[ОШИБКА] Не удалось инициализировать таблицу posts")
            return

        # 4. Подготовка данных для сохранения
        data_to_save = []
        for post in posts_list:
            # Проверяем наличие обязательного поля ID
            if 'id' not in post:
                logger.warning(f"[ПРЕДУПРЕЖДЕНИЕ] Пост без ID пропущен: {post.get('title', 'Неизвестно')}")
                continue
                
            # Обработка реакций - в новой версии API реакции могут быть объектом
            reactions = post.get('reactions', 0)
            if isinstance(reactions, dict):
                # Если реакции в формате {likes: X, dislikes: Y}, просто берем общее количество
                reactions = sum(reactions.values())

            # Преобразуем данные поста в кортеж для вставки в БД
            post_tuple = (
                post.get('id'),                      # api_id
                post.get('title'),                   # title
                post.get('body'),                    # body
                post.get('userId'),                  # userId
                json.dumps(post.get('tags', [])),    # tags (как JSON-строка)
                reactions                            # reactions (число)
            )
            data_to_save.append(post_tuple)

        # 5. Сохраняем данные в БД
        saved_count = save_data_to_db(conn, table_name, data_to_save, posts_columns)
        
        # 6. Выводим итог
        total_count = len(posts_list)
        if saved_count > 0:
            logger.info(f"[ЗАВЕРШЕНО] Обработка постов успешно завершена. "
                      f"Добавлено {saved_count} из {total_count} записей.")
        else:
            logger.info(f"[ИНФОРМАЦИЯ] Обработка постов завершена. Новые записи не добавлены.")

    finally:
        # 7. Закрываем соединение с БД
        conn.close()
        logger.info("Соединение с базой данных закрыто")


# ========================
# === ТОЧКА ВХОДА ===
# ========================

if __name__ == "__main__":
    """
    Главная точка входа при запуске скрипта напрямую.
    Здесь выполняются основные функции обработки данных.
    """
    # Удаляем существующую БД для чистого старта, если передан аргумент --clean
    if len(sys.argv) > 1 and sys.argv[1] == '--clean' and os.path.exists(DB_NAME):
        try:
            os.remove(DB_NAME)
            print(f"[ИНФОРМАЦИЯ] База данных {DB_NAME} удалена для чистого запуска.")
        except OSError as e:
            print(f"[ОШИБКА] Не удалось удалить базу данных: {e}")

    print("Запуск обработки данных из DummyJSON API...")
    
    # Обрабатываем продукты
    process_products(search_query="iPhone")
    
    # Обрабатываем пользователей
    process_users(limit=20)
    
    # Обрабатываем посты
    process_posts(limit=30)
    
    print("Обработка данных завершена!")
    print(f"Для просмотра базы данных запустите: python dummy/view_db.py")