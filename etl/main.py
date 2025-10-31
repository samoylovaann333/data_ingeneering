import argparse
import sys
import os

# Добавляем путь для импорта модулей
sys.path.append(os.path.dirname(__file__))

from extract import load_data as extract_data
from transform import transform_data
from load import load_data as load_to_db

def run_etl_pipeline(input_path: str, db_path: str = 'medical_data.db'):
    """
    Run complete ETL pipeline: Extract -> Transform -> Load
    
    Args:
        input_path: Path to input data file
        db_path: Path to SQLite database
    """
    print("=" * 60)
    print(" ЗАПУСК ETL ПАЙПЛАЙНА")
    print("=" * 60)
    
    try:
        # Extract
        print("\n ЭТАП 1: EXTRACT")
        print("-" * 30)
        raw_df = extract_data(input_path)
        
        # Transform
        print("\n ЭТАП 2: TRANSFORM")
        print("-" * 30)
        transformed_df = transform_data(raw_df)
        
        # Load
        print("\n ЭТАП 3: LOAD")
        print("-" * 30)
        load_to_db(transformed_df, db_path)
        
        print("\n" + "=" * 60)
        print(" ETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
        print("=" * 60)
        print("📁 Результаты:")
        print(f"   • Сырые данные: data/raw/raw_data.csv")
        print(f"   • Обработанные данные: data/processed/processed_data.parquet")
        print(f"   • База данных: {db_path}")
        print(f"   • Таблица: medical_data (100 записей)")
        
    except Exception as e:
        print(f"\n ОШИБКА В ПАЙПЛАЙНЕ: {e}")
        sys.exit(1)

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description='ETL Pipeline for Medical Data Processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  python -m etl.main --input "data.csv"
  python -m etl.main --input "data.parquet" --db "my_database.db"
  python -m etl.main --input "https://example.com/data.csv"
        '''
    )
    
    parser.add_argument(
        '--input', 
        required=True,
        help='Путь к исходным данным (CSV, Parquet) или URL'
    )
    
    parser.add_argument(
        '--db', 
        default='medical_data.db',
        help='Путь к SQLite базе данных (по умолчанию: medical_data.db)'
    )
    
    args = parser.parse_args()
    
    # Запускаем ETL пайплайн
    run_etl_pipeline(args.input, args.db)

if __name__ == "__main__":
    main()
