import sqlite3

from cdc.CDC import CDC
from demo.DummyDataLakeAdapter import DummyDataLakeAdapter
from demo.SQLiteAdapter import SQLiteRegistryAdapter


def init_db():
    conn = sqlite3.connect("test.db")
    conn.execute("DROP TABLE IF EXISTS products_registry;")
    conn.execute("""CREATE TABLE products_registry 
                    (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name CHAR(255),
                        price FLOAT,
                        description TEXT
                    )""")
    conn.execute(
        """INSERT INTO products_registry (name,price,description) VALUES ("a", 0.0, "desc"), ("b", 0.0, "desc")""")
    conn.commit()
    conn.close()


if __name__ == '__main__':
    init_db()

    print("Launching CDC")
    cdc = CDC(
        SQLiteRegistryAdapter("test.db", "products_registry", ["id"], ["name", "price"]),
        DummyDataLakeAdapter("test_app", "product_registry"),
        strategy="registry",
        transactional=True
    )
    cdc.run()

    # 3 files created: sync.json + one file foreach new row

    print("add new row to db")
    db = sqlite3.connect("test.db")
    db.execute("""INSERT INTO products_registry (name,price,description) VALUES ("c", 50, "cjansdkjc"); """)
    db.commit()

    print("Launching CDC")
    cdc.run()
    print("second run done")

    print("modify existing row in db")
    db = sqlite3.connect("test.db")
    db.execute("""UPDATE products_registry SET name="newC" WHERE name="c"; """)
    db.commit()

    print("Launching cdc")
    cdc.run()
