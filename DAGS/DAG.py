from datetime import datetime, timedelta
from airflow import DAG
# Исправленный импорт для новых версий Airflow
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Настройки DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}


def simple_transfer_all_in_one(**context):
    """Максимально простая версия - безопасное удаление только перенесенных записей"""

    source_hook = PostgresHook(postgres_conn_id='source_postgres_conn')
    target_hook = PostgresHook(postgres_conn_id='target_postgres_conn')

    logging.info("🚀 Начинаем ультра-простой перенос данных...")

    # 1. Создать целевую таблицу
    target_hook.run("""
                    CREATE TABLE IF NOT EXISTS bitcoin_archive
                    (
                        id
                        SERIAL
                        PRIMARY
                        KEY,
                        timestamp
                        TIMESTAMP,
                        price
                        DECIMAL
                    (
                        12,
                        2
                    ),
                        transferred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

    # 2. Взять все данные (с ID для безопасного удаления)
    df = source_hook.get_pandas_df("SELECT id, timestamp, price FROM bitcoin_prices ORDER BY timestamp")

    if df.empty:
        logging.info("⏭️ Нет данных для переноса")
        return

    logging.info(f"📊 Забираем {len(df)} записей для переноса")

    # Сохраняем ID записей для последующего удаления
    record_ids = df['id'].tolist()

    # Для вставки берем только timestamp и price
    insert_df = df[['timestamp', 'price']].copy()

    # 3. Вставить в целевую таблицу
    engine = target_hook.get_sqlalchemy_engine()
    insert_df.to_sql('bitcoin_archive', engine, if_exists='append', index=False)

    logging.info(f"📤 Вставлено {len(insert_df)} записей в архив")

    # 4. Проверить что данные вставились
    inserted = target_hook.get_first(
        "SELECT COUNT(*) FROM bitcoin_archive WHERE transferred_at >= NOW() - INTERVAL '2 minutes'"
    )[0]

    if inserted >= len(df):
        # 5. Безопасно удалить ТОЛЬКО те записи, которые мы забрали
        ids_str = ','.join(map(str, record_ids))
        delete_sql = f"DELETE FROM bitcoin_prices WHERE id IN ({ids_str})"

        deleted_count = source_hook.run(delete_sql)
        logging.info(f"🗑️ Безопасно удалено {len(record_ids)} записей из исходной таблицы")
        logging.info(f"✅ Успешно перенесено {len(df)} записей!")

        # Финальная статистика
        remaining = source_hook.get_first("SELECT COUNT(*) FROM bitcoin_prices")[0]
        total_archive = target_hook.get_first("SELECT COUNT(*) FROM bitcoin_archive")[0]

        logging.info(f"📊 ИТОГ: Осталось в источнике: {remaining}, Всего в архиве: {total_archive}")

    else:
        raise ValueError(f"❌ Ошибка проверки! Ожидалось {len(df)}, найдено {inserted}")


# Создание DAG (исправлено для новых версий Airflow)
simple_dag = DAG(
    'ultra_simple_bitcoin_transfer',
    default_args=default_args,
    description='Ultra simple Bitcoin transfer every 15 minutes',
    schedule=timedelta(minutes=15),  # Изменено с schedule_interval на schedule
    max_active_runs=1,
    tags=['bitcoin', 'simple', 'transfer']
)

# Единственная задача
simple_task = PythonOperator(
    task_id='transfer_all',
    python_callable=simple_transfer_all_in_one,
    dag=simple_dag
)