import pandas as pd
def load_data():
    FILE_ID = "16T11Jo7CuSfgaadz5CLdwTyQRvrDLvSM"
    file_url = f"https://drive.google.com/uc?id={FILE_ID}"
    raw_data = pd.read_csv(file_url)
    print(raw_data.head(10))
