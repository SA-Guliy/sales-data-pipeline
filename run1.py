import os
import sys
import pandas as pd
from pathlib import Path
import configparser
from sales_data import generate_data
from pgdb import PGLoader

def main():
    # Абсолютный путь к конфигу
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.ini')  # Полный путь к конфигу
    
    config = configparser.ConfigParser()
    config.read(config_path)

    # Генерация новых выгрузок
    if generate_data(config) != 0:
        print("ERROR: Data generation failed")
        return 1

    # Импорт данных в PostgreSQL, построчная загрузка в PostgreSQL
    conn = PGLoader(config) 
    csv_dir = Path(base_dir) / config["Files"]["SALES_PATH"].strip()

    # # Загружаем только .csv файлы (лишние файлы игнорируются)
    for csv_file in csv_dir.glob('*.csv'): # glob('*.csv') перебора файлов в директории
        df = pd.read_csv(csv_file)
        conn.upsert_data(df.to_dict('records'))
        #print(f"Inserted {len(df)} rows from {csv_file.name}")

    conn.close()
    print("Импорт завершён!")    

if __name__ == "__main__":
    sys.exit(main())