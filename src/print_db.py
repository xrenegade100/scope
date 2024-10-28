"""
This script connects to an SQLite database (default: `commit_analysis.db`) and retrieves information 
from a table named `commit_analysis`. It performs the following actions:
1. Lists the tables in the database.
2. Retrieves and displays the first few records from the `commit_analysis` table (default limit: 10).
3. Prints these records in a well-formatted grid using the `tabulate` library.
4. Provides basic statistics, such as the total number of records in the table.
5. If the table is `commit_analysis`, additional statistics are displayed:
   - The number of distinct projects.
   - The number of fairness-related commits.
   - The number of distinct authors.

This script is useful for an initial inspection of the `commit_analysis` table's contents and for generating 
basic descriptive statistics about the data.

Attributes:
    db_name (str): Name of the SQLite database file (default is `commit_analysis.db`).
    limit (int): Maximum number of rows to retrieve and display (default is 10).

Usage:
    Run the script directly to see database records and basic statistics.
"""
import sqlite3
from tabulate import tabulate

def print_db_values(db_name='../commit_analysis.db', limit=10):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # List tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_name = 'commit_analysis'
    print(f"\n--- First {limit} records from table {table_name} ---")

    # Retrieve column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]

    # Retrieve data
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cursor.fetchall()

    # Display data using tabulate for better formatting
    print(tabulate(rows, headers=columns, tablefmt="grid"))

    # Print basic statistics
    print(f"\nStatistics for table {table_name}:")
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Total number of records: {count}")

    # If the table is commit_analysis, print additional statistics
    if table_name == 'commit_analysis':
        cursor.execute("SELECT COUNT(DISTINCT project_name) FROM commit_analysis")
        project_count = cursor.fetchone()[0]
        print(f"Number of distinct projects: {project_count}")

        cursor.execute("SELECT COUNT(*) FROM commit_analysis")
        fairness_count = cursor.fetchone()[0]
        print(f"Number of fairness-related commits: {fairness_count}")

        cursor.execute("SELECT COUNT(DISTINCT author) FROM commit_analysis")
        author_count = cursor.fetchone()[0]
        print(f"Number of distinct authors: {author_count}")

    conn.close()

if __name__ == "__main__":
    print_db_values()
