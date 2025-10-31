# Medical Data Engineering Pipeline

Проект по инженерии данных для обработки и анализа медицинских данных о генетических нарушениях.

## О проекте

Проект представляет собой полный ETL пайплайн для обработки медицинских данных, включая загрузку, очистку, трансформацию и анализ данных о генетических нарушениях у пациентов.

## Источники данных
- [Kaggle: Predict the Genetic Disorders Dataset](https://www.kaggle.com/datasets/aibuzz/predict-the-genetic-disorders-datasetof-genomes)
- [Google Drive: Diabetes Dataset](https://drive.google.com/file/d/1aUvCzNoEHzLiKqYh9t9MZ-8wU-qtNFNS/view?usp=drive_link)

**Контекст**: 
Генетические нарушения - заболевания, вызванные мутациями ДНК или хромосомными аномалиями. Проект направлен на анализ медицинских данных для выявления закономерностей и подготовки данных для построения прогнозных моделей.

## Структура проекта

**medical_data_pipeline/**
- **etl/** - Основной ETL пайплайн
  - extract.py - Загрузка данных
  - transform.py - Очистка и трансформация
  - load.py - Сохранение в БД и файлы
  - main.py - CLI интерфейс
  - validate.py - Валидация данных
  - `__init__.py` - Инициализация пакета ETL (версия 1.0.0)
- **notebooks/** - Анализ и визуализация
  - EDA.ipynb - Разведочный анализ данных
  - eda_screenshot.png - Скриншоты анализа
- **experiments/** - Экспериментальные версии
  - api_example/ - Работа с Genomic API
  - data_loader_project/ - Альтернативная загрузка
  - parse_example/ - Примеры парсинга
  - src/ - Вспомогательные утилиты
    - creds.py - Утилиты для работы с БД
    - write_to_db.py - Альтернативная запись в БД
- requirements.txt - Зависимости

## Основные компоненты

### ETL Pipeline (`etl/`)

Полный пайплайн обработки данных от загрузки до сохранения:

- **Extract**: Загрузка данных из Parquet/CSV/URL
- **Transform**: Очистка, приведение типов, обработка пропусков
- **Load**: Сохранение в SQLite и Parquet файлы

**Запуск пайплайна:**
```bash
python -m etl.main --input "path/to/data.parquet" --db "medical_data.db"

## Data Analysis (notebooks/)
Jupyter ноутбуки для анализа данных:

* **Разведочный анализ (EDA)**
* **Визуализация распределений**
* **Анализ корреляций и выбросов**
* **Просмотр EDA анализа**

## Experimental Code (experiments/)
Экспериментальные и учебные реализации:

* **Работа с Genomic API (Ensembl)**
* **Альтернативные методы загрузки данных**
* **Примеры парсинга и обработки**

## Установка и запуск
**Установка зависимостей:**
```bash
pip install -r requirements.txt
```

**Основные зависимости:**
* pandas
* sqlalchemy
* pyarrow
* seaborn
* matplotlib
* jupyter

**Запуск ETL пайплайна:**
```bash
python -m etl.main --input "data/medical_data.parquet"
```

## Результаты работы
После выполнения пайплайна создаются:

* **data/raw/raw_data.csv** - сырые данные
* **data/processed/processed_data.parquet** - обработанные данные
* **medical_data.db** - SQLite база с образцом данных (100 записей)

## Автор
Анна Самойлова

## Лицензия
MIT License