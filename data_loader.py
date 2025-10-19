import pandas as pd
import wget
import os
import numpy as np

class DataLoader:
    def __init__(self):
        self.FILE_ID = "1aUvCzNoEHzLiKqYh9t9MZ-8wU-qtNFNS"
        self.data_folder = "data"
        self.file_path = os.path.join(self.data_folder, "dataset.csv")
        self.df = None
    
    def download_and_load_data(self):
        """Скачивание и загрузка данных с Google Drive"""
        file_url = f"https://drive.google.com/uc?id={self.FILE_ID}"
        
        os.makedirs(self.data_folder, exist_ok=True)
        
        print("Скачивание данных с Google Drive...")
        wget.download(file_url, self.file_path)
        print("\nДанные скачаны!")
        
        self.df = pd.read_csv(self.file_path, encoding='latin1')
        print(f"Загружено данных: {self.df.shape}")
        return self.df
    
    def load_data(self):
        """Загрузка данных"""
        if os.path.exists(self.file_path):
            print("Загрузка существующих данных...")
            self.df = pd.read_csv(self.file_path, encoding='latin1')
            print(f"Загружено: {self.df.shape}")
        else:
            print("Файл не найден, скачиваем с Google Drive...")
            self.download_and_load_data()
        
        return self.df
    
    def clean_special_values(self):
        """Очистка специальных значений перед анализом"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        # Заменяем специальные значения на NaN
        special_values = ['-99', '-', 'Not applicable', 'Not available', '']
        self.df = self.df.replace(special_values, np.nan)
        
        print("Очищены специальные значения")
        return self.df
    
    def auto_clean_problematic_columns(self, null_threshold=0.8, unique_threshold=0.95):
        """Автоматическая очистка проблемных колонок"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("АВТОМАТИЧЕСКАЯ ОЧИСТКА ПРОБЛЕМНЫХ КОЛОНОК:")
        print("=" * 50)
        
        total_rows = len(self.df)
        cleaned_columns = []
        
        for col in self.df.columns:
            col_data = self.df[col]
            null_ratio = col_data.isna().sum() / total_rows
            unique_ratio = col_data.nunique() / total_rows
            
            # Колонки для удаления
            if null_ratio > null_threshold:
                print(f"❌ УДАЛЕНО: {col} ({null_ratio*100:.1f}% пропусков)")
                cleaned_columns.append(col)
                continue
            
            # Колонки с подозрительно высокой уникальностью (возможно ID)
            if unique_ratio > unique_threshold and null_ratio < 0.1:
                print(f"⚠️  ВОЗМОЖНО ID: {col} ({unique_ratio*100:.1f}% уникальных)")
            
            # Заполнение пропусков для категориальных колонок
            if col_data.dtype == 'object' and null_ratio > 0:
                unique_count = col_data.nunique()
                if unique_count < 10:  # Низкая кардинальность
                    most_frequent = col_data.mode()
                    if len(most_frequent) > 0:
                        fill_value = most_frequent[0]
                        self.df[col] = col_data.fillna(fill_value)
                        print(f"✅ ЗАПОЛНЕНО: {col} -> '{fill_value}'")
        
        # Удаляем проблемные колонки
        if cleaned_columns:
            self.df = self.df.drop(columns=cleaned_columns)
            print(f"Удалено колонок: {len(cleaned_columns)}")
        
        print(f"Размер после очистки: {self.df.shape}")
        return self.df
    
    def fix_medical_terminology(self):
        """Нормализация медицинских терминов"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("НОРМАЛИЗАЦИЯ МЕДИЦИНСКИХ ТЕРМИНОВ:")
        
        # Словарь для нормализации
        medical_mapping = {
            'Respiratory Rate (breaths/min)': {
                'Tachypnea': 'High',
                'Normal': 'Normal'
            },
            'Heart Rate (rates/min': {
                'Normal': 'Normal'
            },
            'Blood test result': {
                'slightly abnormal': 'Abnormal',
                'normal': 'Normal'
            }
        }
        
        for col, mapping in medical_mapping.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].replace(mapping)
                print(f"Нормализовано: {col}")
        
        return self.df
    
    def convert_boolean_columns(self):
        """Конвертация колонок с булевыми значениями"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("КОНВЕРТАЦИЯ БУЛЕВЫХ КОЛОНОК:")
        
        boolean_columns = []
        for col in self.df.columns:
            if 'Symptom' in col or 'Test' in col:
                unique_vals = self.df[col].dropna().unique()
                if set(unique_vals).issubset({True, False, 0, 1, 0.0, 1.0}):
                    boolean_columns.append(col)
        
        for col in boolean_columns:
            self.df[col] = self.df[col].astype('bool')
            print(f"Булева: {col}")
        
        return self.df
    
    def analyze_data(self):
        """Детальный анализ данных перед конвертацией"""
        if self.df is None:
            raise ValueError("Сначала загрузите данные!")
        
        # Сначала очистим специальные значения
        self.clean_special_values()
        
        print("ДЕТАЛЬНЫЙ АНАЛИЗ ДАННЫХ:")
        print("=" * 50)
        
        analysis_results = {}
        
        for col in self.df.columns:
            col_data = self.df[col]
            
            # Базовая информация
            total_count = len(col_data)
            null_count = col_data.isna().sum()
            unique_count = col_data.nunique()
            
            # Пробуем числовую конвертацию
            numeric_values = pd.to_numeric(col_data, errors='coerce')
            numeric_count = numeric_values.notna().sum()
            nan_count = numeric_values.isna().sum()
            
            # Анализ нечисловых значений для смешанных типов
            non_numeric_values = []
            if numeric_count > 0 and numeric_count < total_count:
                non_numeric_mask = numeric_values.isna() & col_data.notna()
                non_numeric_values = col_data[non_numeric_mask].unique().tolist()
                if len(non_numeric_values) > 5:  # Ограничиваем вывод
                    non_numeric_values = non_numeric_values[:5] + ['...']
            
            # Анализ типа данных
            if numeric_count == total_count:  # Все значения числовые
                data_type = "numeric_all"
                if (numeric_values % 1 == 0).all():
                    subtype = "integer"
                else:
                    subtype = "float"
            elif numeric_count > 0 and numeric_count < total_count:  # Смешанные
                data_type = "numeric_mixed"
                subtype = "mixed"
            else:  # Текстовые
                data_type = "text"
                if unique_count / total_count < 0.3:
                    subtype = "low_cardinality"
                else:
                    subtype = "high_cardinality"
            
            analysis_results[col] = {
                'total_count': total_count,
                'null_count': null_count,
                'unique_count': unique_count,
                'numeric_count': numeric_count,
                'nan_count': nan_count,
                'data_type': data_type,
                'subtype': subtype,
                'sample_values': col_data.head(3).tolist(),
                'non_numeric_values': non_numeric_values
            }
            
            # Вывод информации о колонке
            print(f"{col}:")
            print(f"   Тип: {data_type} ({subtype})")
            print(f"   Null: {null_count}/{total_count} ({null_count/total_count*100:.1f}%)")
            print(f"   Уникальных: {unique_count} ({unique_count/total_count*100:.1f}%)")
            if numeric_count > 0:
                print(f"   Числовых: {numeric_count} ({numeric_count/total_count*100:.1f}%)")
            if non_numeric_values:
                print(f"   Нечисловые значения: {non_numeric_values}")
            print(f"   Пример: {col_data.head(3).tolist()}")
        
        return analysis_results
    
    def smart_data_preprocessing(self):
        """Умная предобработка данных перед анализом"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("УМНАЯ ПРЕДОБРАБОТКА ДАННЫХ:")
        print("=" * 50)
        
        original_shape = self.df.shape
        
        # 1. Очистка специальных значений
        self.clean_special_values()
        
        # 2. Автоматическая очистка проблемных колонок
        self.auto_clean_problematic_columns()
        
        # 3. Нормализация медицинских терминов
        self.fix_medical_terminology()
        
        # 4. Конвертация булевых колонок
        self.convert_boolean_columns()
        
        final_shape = self.df.shape
        print(f"Результат предобработки: {original_shape} -> {final_shape}")
        
        return self.df
    
    def convert_data_types_smart(self):
        """Умное приведение типов на основе анализа"""
        if self.df is None:
            raise ValueError("Сначала загрузите данные!")
        
        print("УМНОЕ ПРИВЕДЕНИЕ ТИПОВ ДАННЫХ:")
        print("=" * 50)
        
        # Сначала предобработка данных
        self.smart_data_preprocessing()
        
        # Анализируем данные
        analysis = self.analyze_data()
        
        memory_before = self.df.memory_usage(deep=True).sum() / 1024**2
        print(f"Память до оптимизации: {memory_before:.2f} MB")
        
        df_optimized = self.df.copy()
        
        for col, info in analysis.items():
            try:
                data_type = info['data_type']
                subtype = info['subtype']
                
                if data_type == "numeric_all":
                    # Все значения числовые
                    numeric_values = pd.to_numeric(df_optimized[col], errors='coerce')
                    
                    if subtype == "integer":
                        # Целые числа - можем безопасно конвертировать
                        min_val = numeric_values.min()
                        max_val = numeric_values.max()
                        
                        if min_val >= 0:
                            if max_val <= 255:
                                df_optimized[col] = numeric_values.astype('uint8')
                                print(f"{col} -> uint8 (целые, 0-255)")
                            elif max_val <= 65535:
                                df_optimized[col] = numeric_values.astype('uint16')
                                print(f"{col} -> uint16 (целые, 0-65535)")
                            else:
                                df_optimized[col] = numeric_values.astype('uint32')
                                print(f"{col} -> uint32 (целые, >65535)")
                        else:
                            df_optimized[col] = numeric_values.astype('int32')
                            print(f"{col} -> int32 (целые со знаком)")
                    else:
                        # Дробные числа
                        df_optimized[col] = numeric_values.astype('float32')
                        print(f"{col} -> float32 (дробные)")
                
                elif data_type == "numeric_mixed":
                    # Смешанные данные - используем float для безопасности
                    numeric_values = pd.to_numeric(df_optimized[col], errors='coerce')
                    df_optimized[col] = numeric_values.astype('float32')
                    print(f"{col} -> float32 (смешанные, {info['nan_count']} NaN)")
                
                else:  # text
                    # Текстовые данные
                    if subtype == "low_cardinality":
                        df_optimized[col] = df_optimized[col].astype('category')
                        print(f"{col} -> category ({info['unique_count']} уникальных)")
                    else:
                        df_optimized[col] = df_optimized[col].astype('string')
                        print(f"{col} -> string")
                        
            except Exception as e:
                print(f"Ошибка в колонке {col}: {e}")
                continue
        
        self.df = df_optimized
        
        memory_after = self.df.memory_usage(deep=True).sum() / 1024**2
        memory_saved = memory_before - memory_after
        
        print(f"Память после оптимизации: {memory_after:.2f} MB")
        print(f"Экономия памяти: {memory_saved:.2f} MB ({memory_saved/memory_before*100:.1f}%)")
        
        print("Типы данных приведены!")
        return self.df

    def convert_data_types(self):
        """Стандартное приведение типов (для обратной совместимости)"""
        return self.convert_data_types_smart()
    
    def save_data(self, file_path=None, format='parquet'):
        """Сохранение данных"""
        if self.df is None:
            raise ValueError("Нет данных для сохранения!")
        
        if file_path is None:
            file_path = os.path.join(self.data_folder, f"optimized_dataset.{format}")
        
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
        
        if format.lower() == 'parquet':
            self.df.to_parquet(file_path, index=False)
            print(f"Сохранено в Parquet: {file_path}")
        elif format.lower() == 'csv':
            self.df.to_csv(file_path, index=False)
            print(f"Сохранено в CSV: {file_path}")
        else:
            raise ValueError(f"Неизвестный формат: {format}")
        
        return file_path

    def show_info(self):
        """Показать информацию о данных"""
        if self.df is not None:
            print(f"ИНФОРМАЦИЯ О ДАННЫХ:")
            print(f"Размер данных: {self.df.shape}")
            print(f"Типы данных:")
            for col, dtype in self.df.dtypes.items():
                null_count = self.df[col].isna().sum()
                unique_count = self.df[col].nunique()
                print(f"  - {col}: {dtype} (nulls: {null_count}, uniques: {unique_count})")

    def validate_columns(self, expected_columns=None):
        """Валидация набора колонок для защиты от изменений в данных"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        if expected_columns is None:
            # Используем ФАКТИЧЕСКИЕ названия колонок из данных
            expected_columns = [
                'Patient Id', 'Patient Age', 'Genes in mother\'s side', 
                'Inherited from father', 'Maternal gene', 'Paternal gene',
                'Blood cell count (mcL)', 'Patient First Name', 'Family Name',
                'Father\'s name', 'Mother\'s age', 'Father\'s age', 
                'Institute Name', 'Location of Institute', 'Status',
                'Respiratory Rate (breaths/min)', 'Heart Rate (rates/min',  # Фактическое название
                'Test 1', 'Test 2', 'Test 3', 'Test 4', 'Test 5',
                'Parental consent', 'Follow-up', 'Gender', 'Birth asphyxia',
                'Autopsy shows birth defect (if applicable)', 'Place of birth',
                'Folic acid details (peri-conceptional)', 'H/O serious maternal illness',
                'H/O radiation exposure (x-ray)', 'H/O substance abuse',
                'Assisted conception IVF/ART', 'History of anomalies in previous pregnancies',
                'No. of previous abortion', 'Birth defects',
                'White Blood cell count (thousand per microliter)', 'Blood test result',
                'Symptom 1', 'Symptom 2', 'Symptom 3', 'Symptom 4', 'Symptom 5'
            ]
        
        current_columns = list(self.df.columns)
        
        print("ВАЛИДАЦИЯ КОЛОНОК:")
        print(f"Ожидается: {len(expected_columns)} колонок")
        print(f"Фактически: {len(current_columns)} колонок")
        
        # Поиск отсутствующих колонок
        missing_columns = set(expected_columns) - set(current_columns)
        if missing_columns:
            print(f"Отсутствующие колонки: {list(missing_columns)}")
        
        # Поиск новых колонок
        extra_columns = set(current_columns) - set(expected_columns)
        if extra_columns:
            print(f"Новые колонки: {list(extra_columns)}")
        
        # Совпадение колонок
        matching_columns = set(expected_columns) & set(current_columns)
        print(f"Совпадающие колонки: {len(matching_columns)}")
        
        # Проверка точного совпадения названий
        exact_match = set(expected_columns) == set(current_columns)
        print(f"Точное совпадение: {exact_match}")
        
        return {
            'missing': list(missing_columns),
            'extra': list(extra_columns),
            'matching': list(matching_columns),
            'is_valid': len(missing_columns) == 0 and exact_match,
            'exact_match': exact_match
        }

    def set_column_types(self, column_types):
        """Явное приведение типов по заданной схеме"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("ЯВНОЕ ПРИВЕДЕНИЕ ТИПОВ ПО СХЕМЕ:")
        
        # Сначала очистим данные
        self.clean_special_values()
        
        memory_before = self.df.memory_usage(deep=True).sum() / 1024**2
        
        for col, dtype in column_types.items():
            if col in self.df.columns:
                try:
                    if dtype in ['category', 'string']:
                        self.df[col] = self.df[col].astype(dtype)
                    else:
                        # Для числовых типов сначала конвертируем
                        numeric_val = pd.to_numeric(self.df[col], errors='coerce')
                        self.df[col] = numeric_val.astype(dtype)
                    print(f"{col} -> {dtype}")
                except Exception as e:
                    print(f"Ошибка в {col}: {e}")
            else:
                print(f"Колонка {col} не найдена")
        
        memory_after = self.df.memory_usage(deep=True).sum() / 1024**2
        print(f"Экономия памяти: {memory_before - memory_after:.2f} MB")

    def get_recommended_types(self):
        """Рекомендация типов на основе анализа данных"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        analysis = self.analyze_data()
        recommended_types = {}
        
        for col, info in analysis.items():
            if info['data_type'] == 'numeric_all':
                if info['subtype'] == 'integer':
                    min_val = self.df[col].min()
                    max_val = self.df[col].max()
                    if min_val >= 0:
                        if max_val <= 255:
                            recommended_types[col] = 'uint8'
                        elif max_val <= 65535:
                            recommended_types[col] = 'uint16'
                        else:
                            recommended_types[col] = 'uint32'
                    else:
                        recommended_types[col] = 'int32'
                else:
                    recommended_types[col] = 'float32'
            elif info['data_type'] == 'numeric_mixed':
                recommended_types[col] = 'float32'
            else:
                if info['unique_count'] / info['total_count'] < 0.3:
                    recommended_types[col] = 'category'
                else:
                    recommended_types[col] = 'string'
        
        return recommended_types

    def get_data_quality_report(self):
        """Полный отчет о качестве данных"""
        if self.df is None:
            raise ValueError("Данные не загружены")
        
        print("ОТЧЕТ О КАЧЕСТВЕ ДАННЫХ:")
        print("=" * 50)
        
        total_rows = len(self.df)
        total_columns = len(self.df.columns)
        
        # Статистика по пропущенным значениям
        null_stats = self.df.isnull().sum()
        high_null_columns = null_stats[null_stats > total_rows * 0.5]  # >50% пропусков
        
        print(f"Общая статистика:")
        print(f"  Строк: {total_rows}")
        print(f"  Колонок: {total_columns}")
        print(f"  Колонок с >50% пропусков: {len(high_null_columns)}")
        
        if len(high_null_columns) > 0:
            print(f"  Проблемные колонки: {list(high_null_columns.index)}")
        
        # Валидация колонок
        validation = self.validate_columns()
        print(f"Валидация: {'ПРОЙДЕНА' if validation['is_valid'] else 'НЕ ПРОЙДЕНА'}")
        
        return {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'high_null_columns': list(high_null_columns.index),
            'validation': validation
        }