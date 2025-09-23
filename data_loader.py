import pandas as pd
FILE_ID = "1aUvCzNoEHzLiKqYh9t9MZ-8wU-qtNFNS"
file_url = f"https://drive.google.com/uc?id={FILE_ID}"

raw_data = pd.read_csv(file_url, encoding='latin1')

print(raw_data.head(10))
