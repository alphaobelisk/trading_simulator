import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
import warnings

# Подавляем предупреждения pandas
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')


class SimpleBitcoinChart:
    def __init__(self, db_config):
        """Простая инициализация с подключением к PostgreSQL"""
        self.db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(self.db_url)
        print("✅ Подключение к PostgreSQL установлено")

    def get_data(self, limit=1000):
        """Получить данные из БД (последние N записей)"""
        try:
            # Простой способ - подставляем число прямо в запрос
            query = f"""
                SELECT timestamp, price
                FROM bitcoin_prices
                ORDER BY timestamp DESC
                LIMIT {limit}
            """

            # Используем engine напрямую
            df = pd.read_sql(query, self.engine)

            # Сортируем по времени
            df = df.sort_values('timestamp').reset_index(drop=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            print(f"📊 Загружено {len(df)} записей")
            return df

        except Exception as e:
            print(f"❌ Ошибка получения данных: {e}")
            print("🔧 Попробуем альтернативный способ...")
            return self._get_data_alternative(limit)

    def create_chart(self, df):
        """Создать простой линейный график"""
        if df is None or df.empty:
            print("❌ Нет данных для графика")
            return None

        # Создаем простой линейный график
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['price'],
            mode='lines',
            name='Bitcoin Price',
            line=dict(color='#f7931a', width=2),  # Биткоин оранжевый
            hovertemplate='<b>$%{y:,.2f}</b><br>%{x}<extra></extra>'
        ))

        # Простые настройки
        fig.update_layout(
            title="Bitcoin Price Chart",
            xaxis_title="Время",
            yaxis_title="Цена (USD)",
            template="plotly_dark",
            height=600,
            showlegend=False
        )

        return fig

    def show_stats(self, df):
        """Показать простую статистику"""
        if df is None or df.empty:
            return

        current_price = df['price'].iloc[-1]
        min_price = df['price'].min()
        max_price = df['price'].max()
        first_price = df['price'].iloc[0]

        change = current_price - first_price
        change_pct = (change / first_price) * 100

        print("\n📈 СТАТИСТИКА BITCOIN:")
        print(f"💰 Текущая цена: ${current_price:,.2f}")
        print(f"📊 Мин: ${min_price:,.2f} | Макс: ${max_price:,.2f}")
        print(f"🔄 Изменение: ${change:+,.2f} ({change_pct:+.2f}%)")
        print(f"📅 Период: {df['timestamp'].iloc[0]} - {df['timestamp'].iloc[-1]}")

    def _get_data_alternative(self, limit):
        """Альтернативный способ получения данных через psycopg2"""
        try:
            import psycopg2

            # Прямое подключение
            conn = psycopg2.connect(
                host='localhost',
                port='5432',
                database='postgres',
                user='postgres',
                password='192021'
            )

            query = """
                    SELECT timestamp, price
                    FROM bitcoin_prices
                    ORDER BY timestamp DESC
                        LIMIT %s \
                    """

            df = pd.read_sql(query, conn, params=(limit,))
            conn.close()

            df = df.sort_values('timestamp').reset_index(drop=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            print(f"📊 Загружено {len(df)} записей (альтернативный способ)")
            return df

        except Exception as e:
            print(f"❌ И альтернативный способ не работает: {e}")
            return None


# Основной код
if __name__ == "__main__":

    # Настройки базы данных
    DB_CONFIG = {
        'user': 'postgres',
        'password': '192021',
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres'
    }

    # Создаем визуализатор
    chart = SimpleBitcoinChart(DB_CONFIG)

    # Получаем данные (последние 1000 записей)
    data = chart.get_data(limit=1000)

    if data is not None:
        # Показываем статистику
        chart.show_stats(data)

        # Создаем и показываем график
        print("\n📈 Создаем график...")
        bitcoin_chart = chart.create_chart(data)

        if bitcoin_chart:
            bitcoin_chart.show()  # Открывается в браузере
            print("✅ График успешно отображен!")
        else:
            print("❌ Не удалось создать график")
    else:
        print("❌ Нет данных для отображения")

print("\n📋 Для работы нужны: pip install plotly pandas sqlalchemy psycopg2-binary")