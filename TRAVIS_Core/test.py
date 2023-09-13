# import sqlite3
# import threading
# import multiprocessing
# from concurrent.futures import ThreadPoolExecutor

# def create_table():
#     conn = sqlite3.connect("test.db")
#     cursor = conn.cursor()
#     cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)")
#     conn.commit()
#     conn.close()

# def insert_data(thread_id):
#     conn = sqlite3.connect("test.db")
#     cursor = conn.cursor()
#     for i in range(10):
#         cursor.execute("INSERT INTO test_table (value) VALUES (?)", (f"Value {thread_id}_{i}",))
#     conn.commit()
#     conn.close()
#     print(f"Thread {thread_id} inserted 10 rows")

# def process_function(process_id):
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         for thread_id in range(10):
#             executor.submit(insert_data, f"P{process_id}_T{thread_id}_{executor._thread_name_prefix}")

# if __name__ == "__main__":
#     create_table()

#     processes = []
#     for process_id in range(5):
#         process = multiprocessing.Process(target=process_function, args=(process_id,))
#         processes.append(process)
#         process.start()

#     for process in processes:
#         process.join()


import sqlite3
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

class Database:
    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

def create_table():
    with Database("test.db") as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)")
        conn.commit()

def insert_data(thread_id):
    with Database("test.db") as conn:
        cursor = conn.cursor()
        for i in range(10):
            cursor.execute("INSERT INTO test_table (value) VALUES (?)", (f"Value {thread_id}_{i}",))
        conn.commit()
        print(f"Thread {thread_id} inserted 10 rows")

def process_function(process_id):
    with ThreadPoolExecutor(max_workers=10) as executor:
        for thread_id in range(10):
            executor.submit(insert_data, f"P{process_id}_T{thread_id}")

if __name__ == "__main__":
    create_table()

    processes = []
    for process_id in range(5):
        process = multiprocessing.Process(target=process_function, args=(process_id,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()