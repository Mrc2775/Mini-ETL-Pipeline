# Mini ETL Pipeline
# 1. pulls book data from some open library api i found
# 2. cleans it up (takes out the stuff we don't need)
# 3. dumps it into a sqlite database
# 4. runs some queries on it
# 5. exports everything to csv so the prof can open it in excel

import requests
import sqlite3
import csv


# hits the open library api and grabs books by subject
def fetch_books(subject, limit):
    url = "https://openlibrary.org/search.json"

    params = {
        "q": f"subject:{subject}",
        "limit": limit
    }

    response = requests.get(url, params=params)
    response.raise_for_status()  # crashes if something goes wrong, which is fine

    data = response.json()
    books = data.get("docs", [])

    return books


# goes through the raw api data and only keeps what we actually care about
def clean_book_data(raw_books):
    cleaned_books = []

    for book in raw_books:
        title = book.get("title")

        # api returns authors as a list for some reason so just grab the first one
        author_list = book.get("author_name")
        author = author_list[0] if author_list else None

        year = book.get("first_publish_year")

        # skip books with no title bc that would be weird
        if title:
            cleaned_books.append({
                "title": title,
                "author": author,
                "year": year
            })

    return cleaned_books


# sets up the sqlite db and creates the books table if it doesn't exist yet
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


# inserts all the cleaned books into the db
def insert_books_into_db(conn, books):
    cursor = conn.cursor()

    # wipe old data first so we don't get duplicates every time we run it
    cursor.execute("DELETE FROM books")

    for book in books:
        cursor.execute("""
            INSERT INTO books (title, author, year)
            VALUES (?, ?, ?)
        """, (book["title"], book["author"], book["year"]))

    conn.commit()


# query 1: how many books per year
def get_books_by_year(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT year, COUNT(*)
        FROM books
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)

    return cursor.fetchall()


# query 2: top 5 authors with the most books
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

    return cursor.fetchall()


# writes query results to a csv file
def save_to_csv(file_name, header, rows):
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"saved to {file_name}")


# --- main ---

subject = "fantasy"
limit = 200

print("running etl pipeline...")
print("fetching books from api...")

# step 1: extract
raw_books = fetch_books(subject, limit)
print(f"got {len(raw_books)} books from api")

# step 2: transform
cleaned_books = clean_book_data(raw_books)
print(f"kept {len(cleaned_books)} books after cleaning")

# step 3: load
print("loading into database...")
conn = create_database()
insert_books_into_db(conn, cleaned_books)
print("done, saved to books.db")

# step 4: query
print("running queries...")
books_by_year = get_books_by_year(conn)
top_authors = get_top_authors(conn)

print("\nbooks per year (first 10):")
for row in books_by_year[:10]:
    print(row)

print("\ntop 5 authors:")
for row in top_authors:
    print(row)

# step 5: export
print("\nexporting to csv...")
save_to_csv("results_by_year.csv", ["year", "book_count"], books_by_year)
save_to_csv("results_top_authors.csv", ["author", "book_count"], top_authors)

conn.close()
print("\ndone!")
