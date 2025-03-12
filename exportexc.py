import mysql.connector
import pandas as pd
from openpyxl import Workbook
from dotenv import load_dotenv
import os

class DatabaseHandler:
    def __init__(self):
        load_dotenv() 
        self.db1_conn = self.connect_to_database(
            os.getenv('DB1_HOST'),
            os.getenv('DB1_USER'),
            os.getenv('DB1_PASSWORD'),
            os.getenv('DB1_NAME')
        )
        self.db2_conn = self.connect_to_database(
            os.getenv('DB2_HOST'),
            os.getenv('DB2_USER'),
            os.getenv('DB2_PASSWORD'),
            os.getenv('DB2_NAME')
        )

    def connect_to_database(self, host, user, password, database):
        return mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    def fetch_data(self, conn, query):
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        return cursor.fetchall()

    def merge_data(self):
        # Fetch data from both databases
        query1 = "SELECT book_id, book_name, publication_year FROM books"
        query2 = "SELECT author_id, author_name FROM authors"

        books_data = self.fetch_data(self.db1_conn, query1)
        authors_data = self.fetch_data(self.db2_conn, query2)

        # Convert to DataFrames
        books_df = pd.DataFrame(books_data)
        authors_df = pd.DataFrame(authors_data)

        # Merge data based on a common column
        merged_df = pd.merge(books_df, authors_df, left_on='book_id', right_on='author_id')

        return merged_df

    def export_to_excel(self, filename='merged_data.xlsx'):
        merged_data = self.merge_data()

        # Writing data using openpyxl
        wb = Workbook()
        ws = wb.active
        ws.title = "Merged Data"

        # Write headers
        headers = list(merged_data.columns)
        ws.append(headers)

        # Write data rows
        for row in merged_data.itertuples(index=False, name=None):
            ws.append(row)

        wb.save(filename)
        print(f"Data successfully exported to {filename}")

if __name__ == "__main__":
    handler = DatabaseHandler()
    handler.export_to_excel()
