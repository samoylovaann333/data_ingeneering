import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

def write_dataset_to_postgres():
    """
    Записывает датасет в таблицу PostgreSQL
    """
    try:
        # 1. Загружаем данные из найденного parquet файла
        df = pd.read_parquet('anna/data_loader_project_clean/data/optimized_dataset.parquet')
        
        # Ограничиваем 100 строками
        df = df.head(100)
        
        print(f"Загружено {len(df)} строк для записи в БД")
        print(f"Колонки: {list(df.columns)}")
        
        # 2. Проверяем наличие переменных окружения
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            print("Переменные окружения не установлены (это нормально при тесте)")
            print("При проверке преподаватель установит: DB_HOST, DB_USER, DB_PASSWORD")
            return
        
        print("Переменные окружения проверены")
        
        # 3. Подключаемся к PostgreSQL используя переменные окружения
        db_config = {
            'host': os.environ['DB_HOST'],
            'port': os.environ.get('DB_PORT', '5432'),
            'database': 'homeworks',
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD']
        }
        
        # 4. Создаем подключение
        connection_string = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        engine = create_engine(connection_string)
        
        # 5. Записываем данные в таблицу 
        table_name = 'samoylova'  
        
        df.to_sql(
            table_name,
            engine,
            schema='public',
            if_exists='replace',
            index=False,
            method='multi'
        )
        
        print(f"Успешно записано {len(df)} строк в таблицу {table_name}")
        print("Домашнее задание 6 успешно выполнено")
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    write_dataset_to_postgres()
