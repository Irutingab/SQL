from DatabaseManager import DatabaseHandler


def main():
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'irutingaboRai1@',
        'database': 'library'
    }
    
    handler = DatabaseHandler(db_config, './storage/new_library.xlsx')
    handler.run()
if __name__ == "__main__":
    main()
