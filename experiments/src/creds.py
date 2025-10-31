import sqlite3

# Подключись к creds.db
conn = sqlite3.connect('creds.db')
cursor = conn.cursor()

# Посмотрим ВСЕ таблицы и их содержимое
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("=== ВСЕ ТАБЛИЦЫ В БАЗЕ ===")
print(tables)

# Для каждой таблицы покажем структуру и данные
for table in tables:
    table_name = table[0]
    print(f"\n=== ТАБЛИЦА: {table_name} ===")
    
    # Структура таблицы
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    print("Структура:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    # Данные таблицы
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
    print("Данные:")
    for row in rows:
        print(f"  {row}")

conn.close()