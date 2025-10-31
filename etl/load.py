import pandas as pd
import os
from sqlalchemy import create_engine, text

def load_data(transformed_df: pd.DataFrame, db_path: str = 'medical_data.db') -> None:
    """
    Load transformed data to SQLite database and save as parquet
    """
    print(" Начало загрузки данных...")
    
    # Проверяем что данные не пустые
    if transformed_df.empty:
        print(" Ошибка: нет данных для загрузки")
        return
    
    # 1. Сохраняем в Parquet (все данные)
    os.makedirs('data/processed', exist_ok=True)
    parquet_path = 'data/processed/processed_data.parquet'
    
    try:
        transformed_df.to_parquet(parquet_path, index=False)
        print(f" Данные сохранены в Parquet: {parquet_path}")
        
        if os.path.exists(parquet_path):
            file_size = os.path.getsize(parquet_path) / 1024 / 1024
            print(f"   • Размер файла: {file_size:.2f} MB")
    except Exception as e:
        print(f" Ошибка сохранения Parquet: {e}")
        raise
    
    # 2. Загружаем в SQLite (максимум 100 строк)
    print(f"  Загрузка в базу данных: {db_path}")
    
    try:
        engine = create_engine(f'sqlite:///{db_path}')
        
        # Берем только 100 строк для БД
        sample_df = transformed_df.head(100).copy()  # Используем .copy() чтобы избежать предупреждений
        
        table_name = 'medical_data'
        
        # Дополнительная проверка типов данных перед записью в SQLite
        for col in sample_df.columns:
            if sample_df[col].dtype == 'object':
                # Убедимся что все строковые значения действительно строки
                sample_df[col] = sample_df[col].astype(str)
                # Заменяем возможные NaN в строках
                sample_df.loc[sample_df[col] == 'nan', col] = 'Unknown'
        
        # Загружаем данные в SQLite
        sample_df.to_sql(
            table_name, 
            engine, 
            if_exists='replace', 
            index=False
        )
        
        # Проверяем что данные записались - правильный способ с text()
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        print(f" Данные загружены в SQLite:")
        print(f"   • Таблица: {table_name}")
        print(f"   • Записей: {row_count}")
        print(f"   • Колонок: {len(sample_df.columns)}")
        
        # Показываем первые 5 колонок
        if len(sample_df.columns) > 0:
            print(f"   • Пример колонок: {list(sample_df.columns[:5])}")
        
    except Exception as e:
        print(f" Ошибка при загрузке в БД: {e}")
        raise
    
    # 3. Финальная валидация
    print(" Финальная валидация...")
    
    # Проверяем Parquet файл
    if os.path.exists(parquet_path):
        try:
            parquet_df = pd.read_parquet(parquet_path)
            if len(parquet_df) == len(transformed_df):
                print("    Parquet файл содержит все данные")
            else:
                print(f"     Parquet: {len(parquet_df)} строк (исходно: {len(transformed_df)})")
        except Exception as e:
            print(f"    Ошибка чтения Parquet: {e}")
    
    # Проверяем что в БД ровно 100 строк или меньше если данных мало
    expected_rows = min(100, len(transformed_df))
    if row_count == expected_rows:
        print(f"    В БД загружено {row_count} строк (как и ожидалось)")
    else:
        print(f"     В БД загружено {row_count} строк (ожидалось {expected_rows})")

if __name__ == "__main__":
    # Тестирование модуля
    import sys
    sys.path.append('..')
    from extract import load_data
    from transform import transform_data
    
    test_df = load_data("/Users/anna/data_loader_project_clean/data/optimized_dataset.parquet")
    transformed_df = transform_data(test_df)
    load_data(transformed_df)
    print(" Модуль load работает корректно")
