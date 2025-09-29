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
        
        print("📥 Скачивание данных с Google Drive...")
        wget.download(file_url, self.file_path)
        print("\n✅ Данные скачаны!")
        
        self.df = pd.read_csv(self.file_path, encoding='latin1')
        print(f"📊 Загружено данных: {self.df.shape}")
        return self.df
    
    def load_data(self):
        """Загрузка данных"""
        if os.path.exists(self.file_path):
            print("📁 Загрузка существующих данных...")
            self.df = pd.read_csv(self.file_path, encoding='latin1')
            print(f"📊 Загружено: {self.df.shape}")
        else:
            print("📥 Файл не найден, скачиваем с Google Drive...")
            self.download_and_load_data()
        
        return self.df
    
    def analyze_data(self):
        """Детальный анализ данных перед конвертацией"""
        if self.df is None:
            raise ValueError("Сначала загрузите данные!")
        
        print("\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ ДАННЫХ:")
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
                'sample_values': col_data.head(3).tolist()
            }
            
            # Вывод информации о колонке
            print(f"\n📋 {col}:")
            print(f"   Тип: {data_type} ({subtype})")
            print(f"   Null: {null_count}/{total_count} ({null_count/total_count*100:.1f}%)")
            print(f"   Уникальных: {unique_count} ({unique_count/total_count*100:.1f}%)")
            if numeric_count > 0:
                print(f"   Числовых: {numeric_count} ({numeric_count/total_count*100:.1f}%)")
            print(f"   Пример: {col_data.head(3).tolist()}")
        
        return analysis_results
    
    def convert_data_types_smart(self):
        """Умное приведение типов на основе анализа"""
        if self.df is None:
            raise ValueError("Сначала загрузите данные!")
        
        print("\n🔧 УМНОЕ ПРИВЕДЕНИЕ ТИПОВ ДАННЫХ:")
        print("=" * 50)
        
        # Анализируем данные
        analysis = self.analyze_data()
        
        memory_before = self.df.memory_usage(deep=True).sum() / 1024**2
        print(f"\n💾 Память до оптимизации: {memory_before:.2f} MB")
        
        df_optimized = self.df.copy()
        
        for col, info in analysis.items():
            try:
                data_type = info['data_type']
                subtype = info['subtype']
                null_count = info['null_count']
                
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
                                print(f"🔢 {col} -> uint8 (целые, 0-255)")
                            elif max_val <= 65535:
                                df_optimized[col] = numeric_values.astype('uint16')
                                print(f"🔢 {col} -> uint16 (целые, 0-65535)")
                            else:
                                df_optimized[col] = numeric_values.astype('uint32')
                                print(f"🔢 {col} -> uint32 (целые, >65535)")
                        else:
                            df_optimized[col] = numeric_values.astype('int32')
                            print(f"🔢 {col} -> int32 (целые со знаком)")
                    else:
                        # Дробные числа
                        df_optimized[col] = numeric_values.astype('float32')
                        print(f"🔢 {col} -> float32 (дробные)")
                
                elif data_type == "numeric_mixed":
                    # Смешанные данные - используем float для безопасности
                    numeric_values = pd.to_numeric(df_optimized[col], errors='coerce')
                    df_optimized[col] = numeric_values.astype('float32')
                    print(f"🔢 {col} -> float32 (смешанные, {info['nan_count']} NaN)")
                
                else:  # text
                    # Текстовые данные
                    if subtype == "low_cardinality":
                        df_optimized[col] = df_optimized[col].astype('category')
                        print(f"🏷️  {col} -> category ({info['unique_count']} уникальных)")
                    else:
                        df_optimized[col] = df_optimized[col].astype('string')
                        print(f"📝 {col} -> string")
                        
            except Exception as e:
                print(f"⚠️  Ошибка в колонке {col}: {e}")
                continue
        
        self.df = df_optimized
        
        memory_after = self.df.memory_usage(deep=True).sum() / 1024**2
        memory_saved = memory_before - memory_after
        
        print(f"\n💾 Память после оптимизации: {memory_after:.2f} MB")
        print(f"💾 Экономия памяти: {memory_saved:.2f} MB ({memory_saved/memory_before*100:.1f}%)")
        
        print("✅ Типы данных приведены!")
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
            print(f"💾 Сохранено в Parquet: {file_path}")
        elif format.lower() == 'csv':
            self.df.to_csv(file_path, index=False)
            print(f"💾 Сохранено в CSV: {file_path}")
        else:
            raise ValueError(f"Неизвестный формат: {format}")
        
        return file_path

    def show_info(self):
        """Показать информацию о данных"""
        if self.df is not None:
            print(f"\n📊 ИНФОРМАЦИЯ О ДАННЫХ:")
            print(f"Размер данных: {self.df.shape}")
            print(f"Типы данных:")
            for col, dtype in self.df.dtypes.items():
                null_count = self.df[col].isna().sum()
                unique_count = self.df[col].nunique()
                print(f"  - {col}: {dtype} (nulls: {null_count}, uniques: {unique_count})")