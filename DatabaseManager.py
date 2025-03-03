from faker import Faker
import openpyxl
from queue import Queue
from DBconnection import DataBaseConnection
from mysql.connector import Error

class DatabaseHandler:
    def __init__(self, db_config, excel_file="./storage/new_library.xlsx", num_reviews=1000000, chunk_size=10000):
        self.db_connection = DataBaseConnection()
        self.conn = self.db_connection.conn
        self.cursor = self.db_connection.cursor
        self.excel_file = excel_file
        self.num_reviews = num_reviews
        self.chunk_size = chunk_size
        self.queue = Queue()
        self.faker = Faker()

    def read_excel_in_chunks(self):
        """Reads Excel file in chunks."""
        if not self.excel_file:
            print("No Excel file provided.")
            return

        wb = openpyxl.load_workbook(self.excel_file, read_only=True)
        sheet = wb.active
        chunk = []

        for i, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=1):
            chunk.append(row)
            if i % self.chunk_size == 0:
                yield chunk
                chunk = []
        
        if chunk:
            yield chunk  # Yield remaining rows

    def insert_data_from_queue(self):
        """Inserts books and authors from queue into the database in batches."""
        if self.queue.empty():
            print("Queue is empty. No data to insert.")
            return

        print("Inserting books and authors into MySQL database in chunks...")
        authors = {}
        books_batch = []

        while not self.queue.empty():
            title, author_name, publication_year = self.queue.get()

            if author_name not in authors:
                self.cursor.execute(
                    "INSERT INTO authors (author_name) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE author_id=LAST_INSERT_ID(author_id)",
                    (author_name,)
                )
                authors[author_name] = self.cursor.lastrowid  
            
            books_batch.append((title, authors[author_name], publication_year))

            if len(books_batch) >= self.chunk_size:
                self.cursor.executemany(
                    "INSERT INTO books (title, author_id, publication_year) VALUES (%s, %s, %s)",
                    books_batch
                )
                self.conn.commit()
                books_batch.clear()

        if books_batch:
            self.cursor.executemany(
                "INSERT INTO books (title, author_id, publication_year) VALUES (%s, %s, %s)",
                books_batch
            )
            self.conn.commit()

        print("Books and authors inserted successfully.")

    def insert_reviews(self):
        """Inserts reviews into the database in chunks, ensuring reviews are linked to books."""
        try:
            self.cursor.execute(f"SELECT book_id FROM books ORDER BY book_id DESC LIMIT {self.chunk_size}")
            book_ids = [row[0] for row in self.cursor.fetchall()]

            if not book_ids:
                print("No books found!")
                return
            
            print(f"Generating {self.chunk_size} reviews for the latest books...")

            # Insert exactly chunk_size reviews per step
            reviews_batch = [
                (self.faker.sentence(), self.faker.name(), self.faker.random.choice(book_ids)) 
                for _ in range(self.chunk_size)
            ]

            self.cursor.executemany(
                "INSERT INTO reviews (review, reviewer_name, book_id) VALUES (%s, %s, %s)", 
                reviews_batch
            )
            self.conn.commit()

            print(f"Inserted {len(reviews_batch)} reviews successfully.")

        except Error as e:
            self.conn.rollback()
            print(f"Error during review insertion: {e}")

    def run(self):
        """Runs the data processing pipeline."""
        books_inserted = False  # Track if books were inserted

        if self.excel_file:
            print(f"Reading Excel file: {self.excel_file}")

            for chunk_index, chunk in enumerate(self.read_excel_in_chunks(), start=1):
                if not chunk:
                    print(f"Chunk {chunk_index} is empty, skipping.")
                    continue  # Skip empty chunks
                
                for row in chunk:
                    self.queue.put(row)  # Enqueue data in small batches

                print(f"\nChunk {chunk_index} ({len(chunk)} rows) ready to insert.")
                user_input = input("Press Enter to insert this chunk, or type 'exit' to stop: ").strip().lower()
                
                if user_input == "exit":
                    print("Stopping insertion process.")
                    break
                
                self.insert_data_from_queue()  # Insert books & authors in this chunk
                self.insert_reviews()  # Insert reviews in this chunk
                books_inserted = True  # Set flag to true

        else:
            print("No Excel file provided. Skipping book insertion.")

        if not books_inserted:
            print("Skipping review insertion as no books were inserted.")
