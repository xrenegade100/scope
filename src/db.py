"""
This script creates a table in the SQLite database `commit_analysis.db` to store project-specific commit data. 
The table, named `project_commits`, includes the following columns:
    - `nome_progetto`: Stores the name of each project (as TEXT).
    - `numero_commit`: Stores the total number of commits associated with each project (as INTEGER).

The `project_commits` table is intended to establish a relationship with another table that contains data on fairness-related commits.
By maintaining a record of the total commits per project, it enables calculations of statistical measures, such as the percentage 
of fairness-related commits in relation to the overall commit count for each project.

The script performs the following steps:
1. Connects to (or creates if not present) the `commit_analysis.db` SQLite database.
2. Creates the `project_commits` table if it does not already exist.
3. Populates the table with predefined project names and their corresponding total commit counts using the `INSERT` SQL command.
4. Commits these changes to the database and then closes the connection.

This setup provides a foundational dataset for further analysis of fairness-related commit metrics across various projects.
"""

import sqlite3

conn = sqlite3.connect('../commit_analysis.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_commits (
        nome_progetto TEXT,
        numero_commit INTEGER,
        PRIMARY KEY (nome_progetto, numero_commit)
    )
''')

righe = [
    ('lale', 1492),
    ('kserve', 1605),
    ('driverlessai-recipes', 2019),
    ('component-library', 2665),
    ('great_expectations', 12709),
    ('triage', 1490),
    ('building-machine-learning-pipelines', 139),
    ('vertex-ai-samples', 3510),
    ('vertex-ai-mlops', 2128),
    ('Interpretable-Machine-Learning-with-Python', 109),
    ('fairness-indicators', 327),
    ('aliyun-openapi-java-sdk', 6754),
    ('ml_privacy_meter', 481),
    ('MachineLearningNotebooks', 1297),
    ('pycaret', 5357)
]

cursor.executemany('''
    INSERT INTO project_commits (nome_progetto, numero_commit)
    VALUES (?, ?)
''', righe)

conn.commit()
conn.close()