import mysql.connector

class LibraryManager:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="irutingaboRai1@",
                database="library"
            )
            self.cursor = self.conn.cursor() 
            print("Connected to the database!")
        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            exit(1)  # Stop execution if connection fails

    def add_book(self):
        title = input("Enter book title: ")
        publication_year = input("Enter publication year: ")

        # Ensure author_id is correctly linked (auto-generated or selected)
        author_name = input("Enter author name: ")

        # Step 1: Check if author exists
        self.cursor.execute("SELECT author_id FROM authors WHERE author_name = %s", (author_name,))
        author = self.cursor.fetchone()

        if author:
            author_id = author[0]
        else:
            # Step 2: Insert author if not found
            self.cursor.execute("INSERT INTO authors (author_name) VALUES (%s)", (author_name,))
            self.conn.commit()  # Save author first
            author_id = self.cursor.lastrowid  # Get the new author_id

        try:
            # Step 3: Insert book with the correct author_id
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

        # Step 1: Check if the book exists
        self.cursor.execute("SELECT book_id FROM books WHERE title = %s", (book_title,))
        book = self.cursor.fetchone()

        if book:
            book_id = book[0]
            review = input("enter you review: ")
            reviewer_name = input("enter your name: ")

            try:
                # Step 2: Insert the review with a valid book_id
                self.cursor.execute(
                    "INSERT INTO reviews (book_id, review, reviewer) VALUES (%s, %s, %s)",
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
            print("No book found with that title. Please add the book first.")

    def edit_book(self):
        title = input("Enter the title of the book you want to edit: ")

        try:
            self.cursor.execute("SELECT * FROM books WHERE title = %s", (title,))
            book = self.cursor.fetchone()

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

                # Fetch and confirm the updated book
                self.cursor.execute("SELECT * FROM books WHERE book_id = %s", (book[0],))
                updated_book = self.cursor.fetchone()
                print("Confirmed updated book:", updated_book)
            else:
                print("Book not found.")
        except mysql.connector.Error as e:
            print(f"Error editing book: {e}")

    def menu(self):
        while True:
            print("\n--- Library Management Menu ---")
            print("1. Add Book")
            print("2. Add Review")
            print("3. Edit Book")
            print("4. Exit")
            choice = input("Choose an option: ")

            if choice == '1':
                self.add_book()
            elif choice == '2':
                self.add_review()
            elif choice == '3':
                self.edit_book()
            elif choice == '4':
                print("Closing database connection. Goodbye!")
                self.cursor.close()
                self.conn.close()
                break
            else:
                print("Invalid choice. Please select again.")

def main():
    handler = LibraryManager()
    handler.menu()

if __name__ == "__main__":
    main()
