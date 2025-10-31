import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def write_dataset_to_postgres():
    try:
        load_dotenv()
        
        # Загружаем данные
        df = pd.read_parquet('/Users/anna/data_loader_project_clean/data/optimized_dataset.parquet')
        
        # Ограничиваем 100 строками
        df = df.head(100)
        
        print(f"Загружено {len(df)} строк для записи в БД")
        print(f"Колонки: {list(df.columns)}")
        print("Первые 3 строки данных:")
        print(df.head(3))
        
        # Проверяем переменные окружения
        required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"Переменные окружения не установлены: {missing_vars}")
            print("Это нормально при локальном тесте")
            return
        
        # Подключаемся к PostgreSQL
        connection_string = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '5432')}/homeworks"
        )
        
        engine = create_engine(connection_string)
        table_name = 'samoylova'  
        
        df.to_sql(
            table_name, 
            engine, 
            schema='public', 
            if_exists='replace', 
            index=False
        )
        
        print(f"Успешно записано {len(df)} строк в таблицу {table_name}")
        
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    write_dataset_to_postgres()