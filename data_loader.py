import pandas as pd
def load_data():
    FILE_ID = "16T11Jo7CuSfgaadz5CLdwTyQRvrDLvSM"
    file_url = f"https://drive.google.com/uc?id={FILE_ID}"
    try:
        raw_data = pd.read_csv(file_url)
        print("Данные загружены")
        print(f"Размер данных: {raw_data.shape}")
        print(f"Колонки: {list(raw_data.columns)}")
        print("\nПервые 10 строк данных:")
        print(raw_data.head(10))
        return raw_data
    except Exception as e:
        print(f"Ошибка: {e}")
        return None

if __name__ == "__main__":
    data = load_data()
