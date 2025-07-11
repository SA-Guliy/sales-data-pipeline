import os
import psycopg2

class PGLoader:
    def __init__(self, config):
        self.host = config["Database"]["HOST"]
        self.port = config["Database"]["PORT"]
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.dbname = os.getenv("DB_NAME")
        self.connect()

    # Соединение с базой PostgreSQL
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname
        )
        self.cur = self.conn.cursor()

    def upsert_data(self, records):
        """
        Вставка (или обновление) данных в таблицу sales по doc_id + item.
        Использует ON CONFLICT — современная комбинация для upsert.
        """
        for rec in records:
            self.cur.execute("""
                INSERT INTO sales1 (
                    doc_id, item, category, amount, price, discount
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id, item) DO UPDATE
                SET amount = EXCLUDED.amount,
                    price = EXCLUDED.price,
                    discount = EXCLUDED.discount
            """, (
                rec["doc_id"],
                rec["item"],
                rec["category"],
                rec["amount"],
                rec["price"],
                rec["discount"]
            ))
        self.conn.commit() # Сохраняем все изменения

    def close(self):
        # Безопасное закрытие соединения
        self.cur.close()
        self.conn.close()    
