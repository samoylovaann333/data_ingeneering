# Data Engineering Projects

Репозиторий для проектов по инженерии данных.

## О датасете

### Genetic Disorders Prediction
Работа с медицинскими данными для прогнозирования генетических нарушений.

**Источники данных:**
- [Kaggle: Predict the Genetic Disorders Dataset](https://www.kaggle.com/datasets/aibuzz/predict-the-genetic-disorders-datasetof-genomes)
- [Google Drive: Diabetes Dataset](https://drive.google.com/file/d/1aUvCzNoEHzLiKqYh9t9MZ-8wU-qtNFNS/view?usp=drive_link)

**Контекст:**
Генетические нарушения - это заболевания, вызванные мутациями ДНК или хромосомными аномалиями. Многие известные болезни связаны с этими унаследованными мутациями. Генетическое тестирование - это важный инструмент, который позволяет пациентам принимать обоснованные решения о профилактике, лечении и раннем выявлении.

С ростом населения исследования показывают тревожное увеличение частоты этих нарушений. Значительным фактором является низкая осведомленность общественности о важности генетического тестирования. Для борьбы с этим и предотвращения трагических исходов крайне важно проводить генетический скрининг во время беременности.

## Структура проекта
data_ingeneering/
├── experiments/ # Экспериментальные и пробные версии кода
│ ├── api_example/ # Пример работы с Genomic API (Ensembl)
│ ├── data_loader_project/ # Пробная версия загрузки данных
│ ├── parse_example/ # Пример парсинга данных
│ └── src/ # Пробные версии скриптов
│ ├── creds.py # Скрипт для проверки учетных данных
│ └── write_to_db.py # Пробная версия записи в БД (ДЗ №6)
├── etl/ # Актуальный ETL пайплайн (ДЗ №8)
├── notebooks/ # Ноутбуки для анализа данных
├── data/ # Данные (исключены из Git)
└── requirements.txt # Зависимости проекта

text

## Проекты

### [experiments/api_example](experiments/api_example/) - Genomics API Reader
Проект для работы с Genomic API (Ensembl). Включает:
- Загрузку данных о генах через REST API
- Обработку и анализ геномных данных
- Визуализацию результатов

**Технологии:** Python, Pandas, Requests, Ensembl REST API

### [experiments/data_loader_project](experiments/data_loader_project/) - Data Loading and Validation
Проект для загрузки и валидации медицинских данных. Включает:
- Загрузку данных из Google Drive
- Валидацию и очистку медицинских данных
- Автоматическое определение типов данных
- Предобработку данных для анализа

**Технологии:** Python, Pandas, Data Validation, ETL processes

### [notebooks](notebooks/) - Exploratory Data Analysis
Jupyter ноутбуки для разведочного анализа данных:
- EDA медицинских данных пациентов (ДЗ №5, №7)
- Анализ структуры и качества данных
- Визуализация распределений и выбросов с использованием Seaborn

**Технологии:** Python, Pandas, Jupyter, Seaborn, Data Analysis

[View EDA notebook on nbviewer](https://nbviewer.org/github/samoylovaann333/data_ingeneering/blob/main/notebooks/EDA.ipynb)

## ETL Pipeline (ДЗ №8)

### Структура пакета `etl`:
- `extract.py` - загрузка данных из Parquet/CSV/URL
- `transform.py` - очистка и трансформация медицинских данных  
- `load.py` - сохранение в SQLite и Parquet
- `main.py` - CLI интерфейс для запуска пайплайна
- `validate.py` - функции валидации данных

### Запуск ETL пайплайна:

```bash
# Используя медицинские данные
python -m etl.main --input "/Users/anna/data_loader_project_clean/data/optimized_dataset.parquet"

# С кастомной базой данных
python -m etl.main --input "data.csv" --db "my_database.db"
Результат выполнения:
Сырые данные: data/raw/raw_data.csv

Обработанные данные: data/processed/processed_data.parquet

База данных: medical_data.db с таблицей medical_data (100 записей)

Домашние задания
ДЗ №5: EDA анализ медицинских данных

ДЗ №6: Запись данных в PostgreSQL (experiments/src/write_to_db.py)

ДЗ №7: Визуализации Seaborn в EDA ноутбуке

ДЗ №8: Полный ETL пайплайн (etl/)

Установка и запуск
Общие зависимости:
bash
pip install -r requirements.txt
Зависимости для ETL пайплайна:
bash
pip install pandas sqlalchemy pyarrow python-dotenv
Автор
Анна Самойлова

Лицензия
Проекты распространяются под лицензией MIT. Подробнее см. в файле LICENSE.
