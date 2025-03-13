import openpyxl
from openpyxl import Workbook
from DBconnection import DataBaseConnection
import mysql.connector
import os

class ExportExcel:
    def __init__(self):
        try:
            self.workbook = Workbook()  
            self.db_connection = DataBaseConnection()
            self.conn = self.db_connection.conn
            self.cursor = self.db_connection.cursor
            print("Connected to the database!")
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            exit(1)

    def get_table_names(self):
        """Retrieve all table names in the database."""
        try:
            self.cursor.execute("SHOW TABLES;")
            return [table[0] for table in self.cursor.fetchall()]
        except mysql.connector.Error as err:
            print(f"Error fetching table names: {err}")
            return []

    def fetch_data(self, table_name):
        """Fetch all data from a specified table."""
        try:
            self.cursor.execute(f"SELECT * FROM {table_name}")
            return self.cursor.fetchall(), [desc[0] for desc in self.cursor.description]
        except mysql.connector.Error as err:
            print(f"Error fetching data from {table_name}: {err}")
            return [], []

    def export_to_excel(self, file_name="./storage/exported_database.xlsx"):
        """Export entire database to an Excel file with multiple sheets."""
        tables = self.get_table_names()

        if not tables:
            print("No tables found in the database.")
            return

        for idx, table in enumerate(tables):
            data, headers = self.fetch_data(table)

            if not data:
                print(f"No data found in table '{table}'. Skipping...")
                continue

            # Create a new sheet or reuse the first one for the first table
            if idx == 0:
                worksheet = self.workbook.active
                worksheet.title = table
            else:
                worksheet = self.workbook.create_sheet(title=table)

            # Insert headers
            worksheet.append(headers)

            # Insert data
            for row in data:
                worksheet.append(row)

        # Ensure folder path exists before saving
        folder_path = os.path.dirname(file_name)
        os.makedirs(folder_path, exist_ok=True)

        # Save file
        self.workbook.save(file_name)
        print(f"Entire database successfully exported to {file_name}")

def main():
    manager = ExportExcel()
    manager.export_to_excel()

if __name__ == "__main__":
    main()
