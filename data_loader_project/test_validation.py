from data_loader import DataLoader

loader = DataLoader()
df = loader.load_data()

# Умная предобработка
df = loader.smart_data_preprocessing()

# Качество после очистки
report = loader.get_data_quality_report()

# Приведение типов
df = loader.convert_data_types_smart()

# Сохранение
loader.save_data('cleaned_data.parquet')