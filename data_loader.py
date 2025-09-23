import pandas as pd
import wget
import os

FILE_ID = "1aUvCzNoEHzLiKqYh9t9MZ-8wU-qtNFNS"
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

# Папка для хранения данных
data_folder = "data"
os.makedirs(data_folder, exist_ok=True)

# Путь для сохранения файла
file_path = os.path.join(data_folder, "dataset.csv")

# Скачиваем файл
wget.download(file_url, file_path)

# Читаем CSV
raw_data = pd.read_csv(file_path, sep=' ', encoding='latin1')
print(raw_data.head(10))
<<<<<<< HEAD


=======
>>>>>>> 234c1f0 (Добавлен скриншот вывода raw_data.head(10))
