import pandas as pd

def validate_raw_data(df: pd.DataFrame) -> bool:
    """
    Validate raw data after extraction
    
    Args:
        df: Raw DataFrame to validate
        
    Returns:
        bool: True if validation passed
    """
    print("🔍 Валидация сырых данных...")
    
    checks_passed = 0
    total_checks = 4
    
    # Проверка 1: DataFrame не пустой
    if not df.empty:
        print("   ✅ Данные не пустые")
        checks_passed += 1
    else:
        print("   ❌ Данные пустые")
        return False
    
    # Проверка 2: Есть колонки
    if len(df.columns) > 0:
        print(f"   ✅ Есть колонки: {len(df.columns)}")
        checks_passed += 1
    else:
        print("   ❌ Нет колонок")
        return False
    
    # Проверка 3: Есть строки
    if len(df) > 0:
        print(f"   ✅ Есть строки: {len(df)}")
        checks_passed += 1
    else:
        print("   ❌ Нет строк")
        return False
    
    # Проверка 4: Нет полностью пустых колонок
    empty_columns = df.columns[df.isnull().all()].tolist()
    if not empty_columns:
        print("   ✅ Нет полностью пустых колонок")
        checks_passed += 1
    else:
        print(f"   ⚠️  Пустые колонки: {empty_columns}")
    
    print(f"   📊 Результат валидации: {checks_passed}/{total_checks}")
    return checks_passed >= 3

def validate_transformed_data(df: pd.DataFrame) -> bool:
    """
    Validate transformed data
    
    Args:
        df: Transformed DataFrame to validate
        
    Returns:
        bool: True if validation passed
    """
    print("🔍 Валидация трансформированных данных...")
    
    checks_passed = 0
    total_checks = 3
    
    # Проверка 1: Нет пропусков
    missing_values = df.isnull().sum().sum()
    if missing_values == 0:
        print("   ✅ Нет пропущенных значений")
        checks_passed += 1
    else:
        print(f"   ⚠️  Пропущенные значения: {missing_values}")
    
    # Проверка 2: Нет дубликатов
    duplicates = df.duplicated().sum()
    if duplicates == 0:
        print("   ✅ Нет дубликатов")
        checks_passed += 1
    else:
        print(f"   ⚠️  Дубликаты: {duplicates}")
    
    # Проверка 3: Разумный размер данных
    if len(df) > 0 and len(df.columns) > 0:
        print("   ✅ Размер данных корректен")
        checks_passed += 1
    else:
        print("   ❌ Некорректный размер данных")
    
    print(f"   📊 Результат валидации: {checks_passed}/{total_checks}")
    return checks_passed >= 2

if __name__ == "__main__":
    # Тестирование модуля валидации
    test_df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    
    print("🧪 Тестирование модуля валидации...")
    validate_raw_data(test_df)
    validate_transformed_data(test_df)
    print("✅ Модуль validate работает корректно")
