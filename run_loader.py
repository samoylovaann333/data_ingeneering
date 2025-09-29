from data_loader import DataLoader

def main():
    print(" Умная обработка медицинских данных...")
    
    loader = DataLoader()
    
    try:
        # Загружаем данные
        print("\n Загрузка данных...")
        df = loader.load_data()
        
        # Умное приведение типов
        print("\n Умное приведение типов...")
        df = loader.convert_data_types()  # ← используем стандартный метод
        
        # Сохраняем
        print("\n Сохранение данных...")
        loader.save_data(format='parquet')
        loader.save_data('data/optimized_smart.csv', format='csv')
        
        print("\n ОБРАБОТКА ЗАВЕРШЕНА!")
        
    except Exception as e:
        print(f" Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()