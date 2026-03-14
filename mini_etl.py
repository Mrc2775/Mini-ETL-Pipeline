# Mini ETL Pipeline Project
# This project does 5 things:
# 1. Gets book data from Open Library API
# 2. Picks only the fields we need
# 3. Saves the data into a SQLite database
# 4. Runs SQL queries on the data
# 5. Exports the query results into CSV files

import requests
import sqlite3
import csv


# This function gets book data from Open Library
def fetch_books(subject, limit):
    url = "https://openlibrary.org/search.json"

    # These are the values we send to the API
    params = {
        "q": f"subject:{subject}",
        "limit": limit
    }

    # Sending request to API
    response = requests.get(url, params=params)

    # This will show an error if the request fails
    response.raise_for_status()

    # Changing JSON response into Python dictionary
    data = response.json()

    # Getting the list of books from "docs"
    books = data.get("docs", [])

    return books


# This function cleans the raw API data
def clean_book_data(raw_books):
    cleaned_books = []

    for book in raw_books:
        title = book.get("title")

        # author_name usually comes as a list
        author_list = book.get("author_name")

        if author_list:
            author = author_list[0]
        else:
            author = None

        year = book.get("first_publish_year")

        # Only keeping the books that have a title
        if title:
            one_book = {
                "title": title,
                "author": author,
                "year": year
            }
            cleaned_books.append(one_book)

    return cleaned_books


# This function creates the database and table
def create_database():
    conn = sqlite3.connect("books.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            author TEXT,
            year INTEGER
        )
    """)

    conn.commit()
    return conn


# This function saves the cleaned books into the table
def insert_books_into_db(conn, books):
    cursor = conn.cursor()

    # Delete old data so we don't keep adding duplicates every time
    cursor.execute("DELETE FROM books")

    for book in books:
        title = book["title"]
        author = book["author"]
        year = book["year"]

        cursor.execute("""
            INSERT INTO books (title, author, year)
            VALUES (?, ?, ?)
        """, (title, author, year))

    conn.commit()


# This function counts how many books are there in each year
def get_books_by_year(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, COUNT(*)
        FROM books
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)

    results = cursor.fetchall()
    return results


# This function finds the top 5 authors with the most books
def get_top_authors(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT author, COUNT(*)
        FROM books
        WHERE author IS NOT NULL
        GROUP BY author
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)

    results = cursor.fetchall()
    return results


# This function saves any results into a CSV file
def save_to_csv(file_name, header, rows):
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # write column names first
        writer.writerow(header)

        # write all the data rows
        writer.writerows(rows)

    print(f"{file_name} has been created.")


# Main part of the program starts here
subject = "fantasy"
limit = 200

print("Starting Mini ETL Pipeline project...")
print("Getting data from Open Library API...")

# Step 1: Extract
raw_books = fetch_books(subject, limit)
print("Number of raw books fetched:", len(raw_books))

# Step 2: Transform
cleaned_books = clean_book_data(raw_books)
print("Number of cleaned books:", len(cleaned_books))

# Step 3: Load
print("Creating database and saving data...")
conn = create_database()
insert_books_into_db(conn, cleaned_books)
print("Data saved into books.db")

# Step 4: Query
print("Running SQL queries...")

books_by_year = get_books_by_year(conn)
top_authors = get_top_authors(conn)

print("\nBooks count by year (first 10 rows):")
for row in books_by_year[:10]:
    print(row)

print("\nTop 5 authors by number of books:")
for row in top_authors:
    print(row)

# Step 5: Export
print("\nSaving results into CSV files...")
save_to_csv("results_by_year.csv", ["year", "book_count"], books_by_year)
save_to_csv("results_top_authors.csv", ["author", "book_count"], top_authors)

# Close database connection
conn.close()
print("\nProject finished successfully.")