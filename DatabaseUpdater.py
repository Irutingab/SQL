import mysql.connector
from DBconnection import DataBaseConnection

class LibraryManager:
    def __init__(self):
        try:
            self.db_connection = DataBaseConnection()
            self.conn = self.db_connection.conn
            self.cursor = self.db_connection.cursor 
            print("Connected to the database!")
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            exit(1)  # Stop execution if connection fails

    def add_book(self):
        title = input("Enter book title: ")
        publication_year = input("Enter publication year: ")
        author_name = input("Enter author name: ")

        # Check if author exists
        self.cursor.execute("SELECT author_id FROM authors WHERE author_name = %s", (author_name,))
        author = self.cursor.fetchone()

        if author:
            author_id = author[0]
        else:
            # Insert author if not found
            self.cursor.execute("INSERT INTO authors (author_name) VALUES (%s)", (author_name,))
            self.conn.commit()  # Save author first
            author_id = self.cursor.lastrowid  # Get the new author_id

        try:
            # Insert book with the correct author_id
            self.cursor.execute(
                "INSERT INTO books (title, publication_year, author_id) VALUES (%s, %s, %s)",
                (title, publication_year, author_id)
            )
            self.conn.commit()
            print("Book added successfully!")

            # Confirm the saved book
            self.cursor.execute("SELECT * FROM books ORDER BY book_id DESC LIMIT 1")
            book = self.cursor.fetchone()
            print("Confirmed saved book:", book)

        except mysql.connector.Error as e:
            print(f"Error adding book: {e}")

    def add_review(self):
        """Add a review for a book, ensuring the book exists first."""
        book_title = input("Enter the title of the book to review: ")

        # Check if the book to review exists
        self.cursor.execute("SELECT book_id FROM books WHERE title = %s", (book_title,))
        book = self.cursor.fetchone()

        if book:
            book_id = book[0]
            review = input("Enter your review: ")
            reviewer_name = input("Enter your name: ")

            try:
                # Insert the review with a valid book_id
                self.cursor.execute(
                    "INSERT INTO reviews (book_id, review, reviewer_name) VALUES (%s, %s, %s)",
                    (book_id, review, reviewer_name)
                )
                self.conn.commit()
                print("Review added successfully!")
                
                # Confirm the saved review
                self.cursor.execute("SELECT * FROM reviews ORDER BY review_id DESC LIMIT 1")
                saved_review = self.cursor.fetchone()
                print("Confirmed saved review:", saved_review)

            except mysql.connector.Error as e:
                print(f"Error adding review: {e}")
        else:
            print("No book found with the provided title.")

    def edit_book(self):
        title = input("Enter the title of the book you want to edit: ")

        try:
            self.cursor.execute("SELECT * FROM books WHERE title = %s", (title,))
            book = self.cursor.fetchone()  # Fetch the result before executing another query

            if book:
                print("Current Details:", book)
                new_title = input("New title (leave blank to keep current): ") or book[1]
                new_publication_year = input("New publication year (leave blank to keep current): ") or book[2]

                self.cursor.execute(
                    "UPDATE books SET title = %s, publication_year = %s WHERE book_id = %s",
                    (new_title, new_publication_year, book[0])
                )
                self.conn.commit()
                print("Book updated successfully!")

                # Fetch again after updating to confirm changes
                self.cursor.execute("SELECT * FROM books WHERE book_id = %s", (book[0],))
                updated_book = self.cursor.fetchone()
                print("Updated book details:", updated_book)
            else:
                print("Book not found.")
                
            self.cursor.fetchall()  # Ensure all remaining results are cleared

        except mysql.connector.Error as e:
            print(f"Error editing book: {e}")
    def delete_book(self):
        """Deletes a book and its reviews from the library."""
        book_to_delete = input("Enter the title of the book you want to delete: ").strip()

        try:
            # check if the book exists
            self.cursor.execute("SELECT book_id FROM books WHERE title = %s", (book_to_delete,))
            book = self.cursor.fetchone()

            if book:
                # Delete reviews associated with the book
                self.cursor.execute("DELETE FROM reviews WHERE book_id = %s", (book[0],))
                self.conn.commit()

                #delete the book itself
                self.cursor.execute("DELETE FROM books WHERE book_id = %s", (book[0],))
                self.conn.commit()

                print(f"Book '{book_to_delete}' and its reviews deleted successfully!")

            else:
                print("Book not found.")

        except mysql.connector.Error as e:
            print(f"Error deleting book: {e}")


    def menu(self):
        while True:
            print("\n--- Library Management Menu ---")
            print("1. Add Book")
            print("2. Add Review")
            print("3. Edit Book")
            print("4. Dele Book")
            print("5. Exit")
            choice = input("Choose an option: ")

            if choice == '1':
                self.add_book()
            elif choice == '2':
                self.add_review()
            elif choice == '3':
                self.edit_book()
            elif choice == '4':
                self.delete_book()
            elif choice == '5':
                print("Closing database connection!")
                self.cursor.close()
                self.conn.close()
                break
            else:
                print("Invalid choice. Please select again.")

def main():
    manager = LibraryManager()
    manager.menu()

if __name__ == "__main__":
    main()
