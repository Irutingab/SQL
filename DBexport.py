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

    def fetch_data(self, table_name):#retrieve each table's data for excel export
        """Fetch all data from a specified table."""
        try:
            self.cursor.execute(f"SELECT * FROM {table_name}")
            return self.cursor.fetchall(), [desc[0] for desc in self.cursor.description]#retrieve all oll the data from a table with their description: headings
        except mysql.connector.Error as err:
            print(f"Error fetching data from {table_name}: {err}")
            return [], []

    def export_to_excel(self, file_name="./storage/exported_database.xlsx"):
        """Export entire database to an Excel file in different sheets."""
        tables = self.get_table_names()

        if not tables:
            print("No tables found in the database.")
            return

        for idx, table_name in enumerate(tables): # iterates through the table names tuples and returns a table name and an index from 0
            data, headers = self.fetch_data(table_name)#returns data as tuples inserted in different headers as column names

            if not data:
                print(f"No data found in table '{table_name}'")
                continue

            # Create a new sheet
            if idx == 0:
                worksheet = self.workbook.create_sheet(title=table_name)

            # Insert headers
            worksheet.append(headers)

            # Insert data
            for row in data:
                worksheet.append(row)

        # Save file
        self.workbook.save(file_name)
        print(f"Entire database successfully exported to {file_name}")

def main():
    manager = ExportExcel()
    manager.export_to_excel()

if __name__ == "__main__":
    main()
