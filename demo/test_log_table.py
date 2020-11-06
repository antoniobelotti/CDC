import sqlite3
import time

from cdc.CDC import CDC
from demo.DummyDataLakeAdapter import DummyDataLakeAdapter
from demo.SQLiteAdapter import SQLiteLogAdapter


def init_db():
    conn = sqlite3.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS products_log;")
    conn.execute("""CREATE TABLE products_log
                    (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name CHAR(255),
                        price FLOAT,
                        description TEXT,
                        last_update DATETIME DEFAULT CURRENT_TIMESTAMP
                    )""")
    conn.execute(
        """INSERT INTO products_log (name,price,description) VALUES ("a", 0.0, "desc"), ("b", 0.0, "desc")""")
    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_db()

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    print("Run cdc")
    cdc = CDC(
        SQLiteLogAdapter("test.db", "products_log", ["name", "price", "description"], "last_update"),
        DummyDataLakeAdapter("test_app", "product_log"),
        strategy="log",
        transactional=False,
        datetime_format=DATETIME_FORMAT
    )
    cdc.run()

    print("Wait 3 sec to update the timestamp")
    time.sleep(3)
    print("Update line")
    db = sqlite3.connect("test.db")
    db.execute(f"""UPDATE products_log SET price=10000, last_update=CURRENT_TIMESTAMP where name="b"; """)
    db.commit()
    db.close()

    print("Run cdc")
    cdc.run()
