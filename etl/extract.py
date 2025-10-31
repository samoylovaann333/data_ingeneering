import pandas as pd
import os

def load_data(source: str) -> pd.DataFrame:
    """
    Extract data from source and save to raw data folder
    
    Args:
        source: Path to data file or URL
        
    Returns:
        pd.DataFrame: Loaded data
    """
    print(f" Загрузка данных из: {source}")
    
    # Определяем тип источника и загружаем данные
    if source.endswith('.parquet'):
        df = pd.read_parquet(source)
        print(" Загружен Parquet файл")
    elif source.endswith('.csv'):
        df = pd.read_csv(source)
        print(" Загружен CSV файл")
    elif source.startswith('http'):
        # Загрузка из интернета
        df = pd.read_csv(source)
        print(" Загружены данные из URL")
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {source}")
    
    # Базовая валидация
    if df.empty:
        raise ValueError("Загруженный файл пустой")
    
    print(f" Размер данных: {df.shape[0]} строк, {df.shape[1]} колонок")
    print(f" Колонки: {list(df.columns)}")
    
    # Сохраняем сырые данные
    os.makedirs('data/raw', exist_ok=True)
    raw_output_path = 'data/raw/raw_data.csv'
    df.to_csv(raw_output_path, index=False)
    print(f" Сырые данные сохранены: {raw_output_path}")
    
    return df

if __name__ == "__main__":
    # Тестирование модуля
    test_df = load_data("/Users/anna/data_loader_project_clean/data/optimized_dataset.parquet")
    print(" Модуль extract работает корректно")
