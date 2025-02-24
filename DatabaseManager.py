from mysql.connector import connect, Error
from faker import Faker
import openpyxl

class DatabaseHandler:
    def __init__(self, db_config, excel_file=None, num_reviews=1000000):
        self.db_config = db_config
        self.excel_file = excel_file
        self.num_reviews = num_reviews
        self.conn = None
        self.cursor = None
        self.faker = Faker()

    def connect_db(self):
        try:
            self.conn = connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Database connection successful!")
        except Error as e:
            print(f"Database connection error: {e}")

    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def insert_reviews(self):
        try:
            batch_size = 1000  # Insert 1000 reviews at a time
            reviews = [(self.faker.text(max_nb_chars=200), self.faker.name()) for _ in range(self.num_reviews)]
            for i in range(0, len(reviews), batch_size):
                self.cursor.executemany("INSERT INTO reviews (review, reviewer) VALUES (%s, %s)", reviews[i:i+batch_size])
                self.conn.commit()
                print(f"Inserted {i + batch_size} reviews so far...")
            print(f"{self.num_reviews} reviews inserted successfully.")
        except Error as e:
            print(f"Error during insertion: {e}")


    def read_excel(self):
        if not self.excel_file:
            print("No Excel file provided.")
            return []
        print("Reading Excel file...")
        wb = openpyxl.load_workbook(self.excel_file)
        sheet = wb.active
        data = [row for row in sheet.iter_rows(min_row=2, values_only=True)]
        print(f"Read {len(data)} rows from the Excel file.")
        return data

    def insert_data_from_excel(self):
        data = self.read_excel()
        if not data:
            return
        print("Inserting data into MySQL database...")
        authors = {}
        for title, author_name, publication_year in data:
            if author_name not in authors:
                self.cursor.execute(
    "INSERT INTO authors (author_name) VALUES (%s) ON DUPLICATE KEY UPDATE author_id=LAST_INSERT_ID(author_id)",
    (author_name,))
                authors[author_name] = self.cursor.lastrowid
        books_data = [(title, authors[author_name], publication_year) for title, author_name, publication_year in data]
        self.cursor.executemany("INSERT INTO books (title, author_id, publication_year) VALUES (%s, %s, %s)", books_data)
        self.conn.commit()

        print("Data insertion completed.")

    def run(self):
        self.connect_db()
        self.insert_data_from_excel()
        self.insert_reviews()
        self.close_db()


