import pandas as pd
import numpy as np

def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean the medical data
    """
    print("🔄 Начало трансформации данных...")
    
    df = raw_df.copy()
    
    print(f"📊 Исходный размер: {df.shape[0]} строк, {df.shape[1]} колонок")
    
    # 1. Приведение типов данных - только для явно числовых колонок
    print("📝 Приведение типов данных...")
    
    # Определяем какие колонки точно числовые по их названиям и содержимому
    numeric_candidates = [
        'Patient Age', 'Blood cell count (mcL)', "Mother's age", "Father's age",
        'Test 1', 'Test 2', 'Test 3', 'Test 4', 'Test 5',
        'No. of previous abortion', 'White Blood cell count (thousand per microliter)'
    ]
    
    numeric_columns = []
    for col in numeric_candidates:
        if col in df.columns:
            try:
                # Пробуем преобразовать в числовой тип
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Проверяем что получились нормальные числа (не все NaN)
                if not df[col].isnull().all():
                    numeric_columns.append(col)
                    print(f"   ✅ {col} -> числовой тип")
                else:
                    print(f"   ❌ {col} -> все значения NaN после преобразования")
            except Exception as e:
                print(f"   ❌ {col} -> ошибка преобразования: {e}")
    
    # 2. Обработка пропущенных значений
    print("🔍 Обработка пропущенных значений...")
    
    # Для числовых колонок заполняем медианой
    for col in numeric_columns:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            # Исправленный способ без inplace
            df.loc[df[col].isnull(), col] = median_val
            print(f"   🔧 {col}: заполнено {df[col].isnull().sum()} пропусков медианой {median_val:.1f}")
    
    # Для категориальных колонок - аккуратная обработка
    categorical_columns = [col for col in df.columns if col not in numeric_columns]
    
    # Обрабатываем только первые 10 категориальных колонок чтобы не перегружать
    for col in categorical_columns[:10]:
        if df[col].isnull().sum() > 0:
            try:
                # Для категориальных данных используем моду
                if df[col].dtype.name == 'category':
                    # Для категориальных типов добавляем новую категорию
                    if 'Unknown' not in df[col].cat.categories:
                        df[col] = df[col].cat.add_categories(['Unknown'])
                    df.loc[df[col].isnull(), col] = 'Unknown'
                else:
                    # Для обычных строковых данных - убедимся что тип object
                    df[col] = df[col].astype(str)
                    df.loc[df[col].isnull(), col] = 'Unknown'
                    df.loc[df[col] == 'nan', col] = 'Unknown'  # Обрабатываем строки 'nan'
                print(f"   🔧 {col}: заполнено пропусков значением 'Unknown'")
            except Exception as e:
                print(f"   ⚠️  {col}: не удалось заполнить пропуски - {e}")
    
    # 3. Обработка дубликатов
    print("🔍 Поиск дубликатов...")
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        df = df.drop_duplicates()
        print(f"   ⚠️  Удалено дубликатов: {duplicates}")
    else:
        print("   ✅ Дубликатов не найдено")
    
    # 4. Удаляем колонки где все значения одинаковые или бесполезные
    print("🔍 Очистка бесполезных колонок...")
    columns_to_drop = []
    for col in df.columns:
        # Колонки где все значения одинаковые
        if df[col].nunique() <= 1:
            columns_to_drop.append(col)
            print(f"   🗑️  Удалена колонка {col} (все значения одинаковые)")
        # Колонки где слишком много пропусков (>90%)
        elif df[col].isnull().sum() / len(df) > 0.9:
            columns_to_drop.append(col)
            print(f"   🗑️  Удалена колонка {col} (>90% пропусков)")
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
    
    # 5. Преобразуем все оставшиеся нечисловые колонки в строки для совместимости с SQLite
    print("🔧 Приведение типов для SQLite...")
    for col in df.columns:
        if col not in numeric_columns:
            try:
                df[col] = df[col].astype(str)
                # Заменяем 'nan' строки на 'Unknown'
                df.loc[df[col] == 'nan', col] = 'Unknown'
                print(f"   ✅ {col} -> строковый тип")
            except Exception as e:
                print(f"   ⚠️  {col}: не удалось преобразовать в строку - {e}")
    
    # 6. Базовая статистика после трансформации
    print("📊 Статистика после трансформации:")
    print(f"   • Размер данных: {df.shape[0]} строк, {df.shape[1]} колонок")
    print(f"   • Числовые колонки: {len(numeric_columns)}")
    print(f"   • Строковые колонки: {len(df.columns) - len(numeric_columns)}")
    
    if numeric_columns:
        print(f"   • Примеры числовых колонок:")
        for col in numeric_columns[:3]:
            if col in df.columns:
                print(f"     {col}: {df[col].min():.1f} - {df[col].max():.1f}")
    
    print("✅ Трансформация данных завершена")
    return df

if __name__ == "__main__":
    # Тестирование модуля
    import sys
    sys.path.append('..')
    from extract import load_data
    
    test_df = load_data("/Users/anna/data_loader_project_clean/data/optimized_dataset.parquet")
    transformed_df = transform_data(test_df)
    print("✅ Модуль transform работает корректно")
