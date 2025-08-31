import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import time


def get_bitcoin_price():
    """Получить текущую цену Bitcoin с Binance API"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Проверка на HTTP ошибки
        data = response.json()
        price = float(data['price'])
        print(f"BTC Price: ${price}")
        return price
    except Exception as e:
        print(f"❌ Ошибка получения цены: {e}")
        return None


def save_to_database(price, engine):
    """Сохранить цену в базу данных"""
    try:
        # Текущее время
        current_time = datetime.now()

        # SQL запрос для вставки (только время и цена)
        query = text("""
                     INSERT INTO bitcoin_prices (timestamp, price)
                     VALUES (:timestamp, :price)
                     """)

        # Выполняем запрос с автокоммитом
        with engine.begin() as conn:  # begin() автоматически делает commit
            conn.execute(query, {
                'timestamp': current_time,
                'price': price
            })

        print(f"✅ Сохранено в БД: ${price} в {current_time.strftime('%H:%M:%S')}")
        return True

    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        return False


def create_table_if_not_exists(engine):
    """Создать таблицу если её нет"""
    try:
        create_table_query = text("""
                                  CREATE TABLE IF NOT EXISTS bitcoin_prices
                                  (
                                      id
                                      SERIAL
                                      PRIMARY
                                      KEY,
                                      timestamp
                                      TIMESTAMP
                                      DEFAULT
                                      CURRENT_TIMESTAMP,
                                      price
                                      DECIMAL
                                  (
                                      12,
                                      2
                                  ) NOT NULL
                                      )
                                  """)

        with engine.begin() as conn:
            conn.execute(create_table_query)

        print("✅ Таблица создана или уже существует")
        return True

    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        return False


# Настройки подключения к PostgreSQL
DB_CONFIG = {
    'user': 'postgres',  # ваш пользователь
    'password': '192021',  # ваш пароль
    'host': 'localhost',  # или IP сервера
    'port': '5432',  # порт PostgreSQL
    'database': 'postgres'  # название вашей БД
}

# Строка подключения
db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Создаем движок SQLAlchemy
try:
    engine = create_engine(db_url, echo=False)  # echo=True для отладки SQL

    # Тестируем подключение
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    print("✅ Подключение к PostgreSQL успешно")

    # Создаем таблицу если нужно
    if not create_table_if_not_exists(engine):
        print("❌ Не удалось создать таблицу")
        exit(1)

except Exception as e:
    print(f"❌ Ошибка подключения к БД: {e}")
    exit(1)

# Основной цикл сбора данных
y = 0
x = 10000
errors = 0

print(f"🚀 Начинаем сбор данных. Планируется собрать {x} записей")

for i in range(x):
    # Получаем цену
    current_price = get_bitcoin_price()

    if current_price is None:
        errors += 1
        print(f"❌ Ошибка получения цены. Пропускаем запись. Ошибок: {errors}")
        time.sleep(5)  # Больше ждем при ошибке
        continue

    y += 1

    # Записываем в БД
    if save_to_database(current_price, engine):
        print(f"Number: {y} ------- ${current_price} ✅ Записано в БД")
    else:
        print(f"Number: {y} ------- ${current_price} ❌ Ошибка записи")
        errors += 1

    if y >= x:
        break

    # Пауза между запросами
    time.sleep(1)

print(f"🎉 Завершено! Обработано {y} записей, ошибок: {errors}")

# Проверяем результаты
try:
    # Общее количество записей
    query = text("SELECT COUNT(*) as count FROM bitcoin_prices")
    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        print(f"📊 Всего записей в БД: {result[0]}")

    # Последние 5 записей
    query = text("""
                 SELECT price, timestamp
                 FROM bitcoin_prices
                 ORDER BY timestamp DESC
                     LIMIT 5
                 """)

    with engine.connect() as conn:
        result = conn.execute(query)
        print("\n📈 Последние 5 записей:")
        for row in result:
            print(f"${row[0]} - {row[1]}")

    # Статистика за сессию
    if y > 0:
        query = text("""
                     SELECT MIN(price) as min_price,
                            MAX(price) as max_price,
                            AVG(price) as avg_price
                     FROM bitcoin_prices
                     WHERE timestamp >= NOW() - INTERVAL '1 hour'
                     """)

        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            if result[0]:  # Если есть данные
                print(f"\n📊 Статистика за последний час:")
                print(f"   Мин: ${result[0]:.2f}")
                print(f"   Макс: ${result[1]:.2f}")
                print(f"   Среднее: ${result[2]:.2f}")

except Exception as e:
    print(f"❌ Ошибка проверки данных: {e}")

finally:
    # Закрываем соединение
    engine.dispose()
    print("🔌 Соединение с БД закрыто")