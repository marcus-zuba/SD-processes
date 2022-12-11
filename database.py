import sqlite3
from datetime import datetime
import os

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS files (
	id integer PRIMARY KEY,
	file_name text NOT NULL UNIQUE,
	file_processing_status text DEFAULT "PRODUCER_QUEUE",
	last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_SQL = """
INSERT INTO files (file_name, last_update)
   VALUES (?, ?)
"""

def setup_database():
    if os.path.exists("database.sqlite"):
        os.remove("database.sqlite")

    conn = sqlite3.connect("database.sqlite")
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)

    filenames = ['test1.txt', 'test2.txt', 'test3.txt']

    for filename in filenames:
        try:
            current_timestamp = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            cursor.execute(INSERT_SQL,(filename, current_timestamp))
        except sqlite3.IntegrityError:
            pass
        
    conn.commit()
    conn.close()

def get_filename_with_processing_status(processing_status):
    conn = sqlite3.connect("database.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT file_name FROM files WHERE file_processing_status=?", (processing_status,))
    filename = cursor.fetchone()
    if filename: 
        filename = filename[0]
    conn.close()
    return filename

def update_processing_status_of_filename(filename, processing_status):
    conn = sqlite3.connect("database.sqlite")
    cursor = conn.cursor()
    cursor.execute("""UPDATE files  SET file_processing_status = ?, 
                                        last_update = ? 
                                    WHERE file_name= ?""", (processing_status, datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), filename))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    setup_database()