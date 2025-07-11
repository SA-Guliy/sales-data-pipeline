import os
import configparser
import random
import pandas as pd
import glob

def generate_data(config):
    """Генерация CSV файлов с продажами"""
    try:
        # Абсолютный путь до папки выгрузок
        dirname = os.path.dirname(os.path.abspath(__file__))
        sales_dir = os.path.join(dirname, config["Files"]["SALES_PATH"].strip())
        os.makedirs(sales_dir, exist_ok=True)  # Создание папки для данных, если нет

        # Удаление старых csv (очистка папки, чтобы не копились файлы)
        for file in glob.glob(os.path.join(sales_dir, '*.csv')):
            os.remove(file)

         # Чтение параметров генерации из конфиг
        total_shops = config.getint('Shop_Network', 'total_shops')
        large_shops = config.getint('Shop_Network', 'large_shops')
        cash_per_large = config.getint('Shop_Network', 'cash_per_large')
        cash_per_small = config.getint('Shop_Network', 'cash_per_small')
        min_rows = config.getint('Shop_Network', 'min_rows')
        max_rows = config.getint('Shop_Network', 'max_rows')

        # Парсинг категорий и диапазонов цен
        categories = {
            cat: [item.strip() for item in items.split(",")]
            for cat, items in config["Category_items"].items()
        }

        price_ranges = {
            cat: (int(config["Price_ranges"][f"{cat}_min"]),
                  int(config["Price_ranges"][f"{cat}_max"]))
            for cat in categories
        }

        # Генерация файлов для всех магазинов и касс
        for shop_num in range(1, total_shops + 1):
            cash_count = cash_per_large if shop_num <= large_shops else cash_per_small

            for cash_num in range(1, cash_count + 1):
                rows = random.randint(min_rows, max_rows)
                data = []
                for i in range(1, rows + 1):
                    doc_id = f"{shop_num}_{cash_num}_{i}"  # Уникальный doc_id
                    category = random.choice(list(categories))
                    data.append({
                        'doc_id': doc_id,
                        'item': random.choice(categories[category]),
                        'category': category,
                        'amount': random.randint(1, 5),
                        'price': random.randint(*price_ranges[category]),
                        'discount': random.randint(0, 20)
                    })

                # Сохранение CSV с абсолютным путем
                output_path = os.path.join(sales_dir, f"{shop_num}_{cash_num}.csv")
                pd.DataFrame(data).to_csv(output_path, index=False, encoding='utf-8')
                print(f"Создан файл: {output_path}")

        return 0
    except Exception as e:
        print(f"Ошибка генерации данных: {str(e)}")
        return 1
