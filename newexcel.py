import openpyxl
from openpyxl import Workbook  
from DBconnection import DataBaseConnection
from dotenv import load_dotenv
import mysql.connector  
import os

class ExportExcel: 
    def __init__(self):
        try:
            self.workbook = None
            self.worksheet = None
            self.db_connection = DataBaseConnection()
            self.conn = self.db_connection.conn
            self.cursor = self.db_connection.cursor 
            print("Connected to the database!")
        except mysql.connector.Error as err:  
            print(f"Database connection error: {err}")
            exit(1)

    def fetch_data(self, query):
        """Fetch data from the specified query"""
        try:
            self.cursor.execute(query)  
            return self.cursor.fetchall(), [desc[0] for desc in self.cursor.description]
        except mysql.connector.Error as err:
            print(f"Error fetching data: {err}")
            return [], []

    def export_to_excel(self, data, headers, file_name="./storage/exported_data.xlsx"):
        """Export data to an Excel file"""
        if not data:
            print("No data found to export.")
            return
        
        self.workbook = Workbook()  
        self.worksheet = self.workbook.active
        self.worksheet.title = "Database Export"

        # Insert headers
        self.worksheet.append(headers)

        # Insert data
        for row in data:
            self.worksheet.append(row)
        
        # Ensure the folder path exists before saving
        folder_path = os.path.dirname(file_name)
        os.makedirs(folder_path, exist_ok=True)
        
        # Save file
        self.workbook.save(file_name)
        print(f"Data successfully exported to {file_name}")

def main():
    manager = ExportExcel()

    # Custom SQL query to select specific columns from multiple tables
    query = """
    SELECT 
        b.title,
        b.publication_year, 
        a.author_name,
        r.review
    FROM books b
    JOIN authors a ON b.author_id = a.author_id
    JOIN reviews r ON b.book_id = r.book_id;
    """

    data, headers = manager.fetch_data(query)
    manager.export_to_excel(data, headers)

if __name__ == "__main__": 
    main()
