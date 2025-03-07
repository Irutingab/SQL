from DatabaseManager import DatabaseHandler
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def main():
    db_config = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME")
    }
                                                
    handler = DatabaseHandler(db_config)
    handler.run()

if __name__ == "__main__":
    main()
