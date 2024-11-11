import base64
import json
import os
import ast
from posixpath import abspath, dirname
import re
import sqlite3
import csv
import logging
from github import Github, Auth, Repository
from typing import List, Dict, Tuple
from tqdm import tqdm
import requests
from dotenv import load_dotenv  # Import dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve GitHub token from environment variables
GH_TOKEN = os.getenv("GH_TOKEN")

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analysis_log.txt"),
        # logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_fairness_identifiers(file_path="../identifiers.csv") -> List[Tuple[str, str, str]]:
    identifiers = []
    
    # Open the CSV file and read each row
    with open(file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            # Append each identifier as a tuple (module_name, identifier_name, type)
            module_name = row["module_name"]
            identifier_name = row["identifier_name"]
            identifier_type = row["type"]
            identifiers.append((module_name, identifier_name, identifier_type))
    
    return identifiers

# Load fairness-related identifiers
fairness_identifiers = load_fairness_identifiers()

def load_identifiers_from_csv(csv_path):
    """
    Load identifiers from a CSV file.
    
    Args:
        csv_path (str): Path to the CSV file containing identifiers.
    
    Returns:
        dict: A dictionary of labeled identifiers.
    """
    identifiers = {}
    try:
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                identifiers[row['identifier_name']] = row['type']
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}")
    except KeyError:
        logger.error("CSV file must contain columns 'module_name', 'identifier_name', 'type'")
    
    return identifiers

def extract_identifier_types(code, identifiers):
    """
    Extract and print identifier types found in the code.
    
    Args:
        code (str): Source code to analyze.
        identifiers (dict): Dictionary of labeled identifiers.
    
    Returns:
        list: List of identified types.
    """
    found_types = []
    try:
        code_ast = ast.parse(code)
        for node in ast.walk(code_ast):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    ident = node.func.attr
                elif isinstance(node.func, ast.Name):
                    ident = node.func.id
                
                # Check if the identifier exists in the dictionary
                if ident in identifiers:
                    found_types.append(identifiers[ident])
    except SyntaxError:
        pass
    
    return found_types

def process_commit(g: Github, project_name, commit_hash, repo_pool: dict[str, Repository.Repository], output_cursor, target_identifiers: List[Tuple[str, str, str]]):
    # First, create the table with the additional flag column
    output_cursor.execute("""
        CREATE TABLE IF NOT EXISTS commit_identifier_types (
            repo_name TEXT,
            commit_hash TEXT,
            file_path TEXT,
            identifier TEXT,
            type TEXT,
            flag TEXT
        )
    """)
    
    # Data structure to store identifier occurrences with their line status
    identifier_occurrences = {
        'added': [],      # Lines starting with '+'
        'removed': [],    # Lines starting with '-'
        'unchanged': []   # Context lines
    }
    
    target_identifier_dict = {(iden): typ for mod, iden, typ in target_identifiers}
    if project_name not in repo_pool:
        repo_pool[project_name] = g.get_repo(project_name)
    repo = repo_pool[project_name]
    commit = repo.get_commit(commit_hash)
    
    # Convert PaginatedList to list and filter for Python files
    all_files = list(commit.files)
    total_files = len(all_files)
    python_files = [f for f in all_files if f.filename.endswith(('.py', '.ipynb'))]
    total_python_files = len(python_files)
    
    logger.info(f"Found {total_python_files} Python/Jupyter files out of {total_files} total files")
    
    # Create main progress bar for files
    with tqdm(total=total_python_files, 
              desc=f"Processing {project_name}:{commit_hash[:7]}", 
              unit="file") as pbar:
        
        # Initialize counters for the progress bar
        identifiers_found = 0
        total_lines_processed = 0
        
        for file_index, file in enumerate(python_files, 1):
            # Update progress bar description with current file
            pbar.set_description(f"File {file_index}/{total_python_files}: {file.filename.split('/')[-1]}")
            
            # Create the necessary directory if it doesn't exist
            file_dir = dirname(abspath(f"error_files/{file.filename}"))
            
            headers = {
                'Authorization': f'token {GH_TOKEN}'  # Replace with your GitHub token if needed
            }
            response = requests.get(url=file.contents_url)

            content = ""
            if response.status_code == 200:
                data = response.json()
                content = base64.b64decode(data["content"]).decode('utf-8')

            # Parse the entire file content
            try:
                if file.filename.endswith('.ipynb'):
                    file_content = get_notebook_content(content)
                else:
                    file_content = content

            except (UnicodeDecodeError, AttributeError):
                logger.error(f"Error decoding file content for {file.filename}")
                pbar.update(1)
                continue
            
            # Process the file content and track line status
            original_lines, code_lines = process_file_content(file_content, file.filename.endswith('.ipynb'))
            try:
                for node in ast.walk(ast.parse(file_content)):
                    ident = None
                    if isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                ident = target.id
                    elif isinstance(node, ast.Call):
                        if isinstance(node.func, ast.Attribute):
                            ident = node.func.attr
                        elif isinstance(node.func, ast.Name):
                            ident = node.func.id
                            
                    if ident is not None and ident in target_identifier_dict:
                        identifiers_found += 1
                        # Get the original line status
                        if node.lineno - 1 < len(original_lines):
                            status, content = original_lines[node.lineno - 1]
                            
                            # Store in our tracking structure
                            identifier_occurrences[status].append({
                                'identifier': ident,
                                'line_number': node.lineno,
                                'content': content,
                                'file': file.filename
                            })
                            
                            # Save to database
                            output_cursor.execute("""
                                INSERT INTO commit_identifier_types 
                                (repo_name, commit_hash, file_path, identifier, type, flag)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                project_name,
                                commit_hash,
                                file.filename,
                                ident,
                                target_identifier_dict[ident],
                                status
                            ))
                            
                            # Update progress bar postfix with statistics
                            pbar.set_postfix({
                                'Identifiers': identifiers_found,
                                'Lines': total_lines_processed,
                                'Added': len(identifier_occurrences['added']),
                                'Removed': len(identifier_occurrences['removed'])
                            })
            except SyntaxError:
                os.makedirs(file_dir, exist_ok=True)
                # Write the file content to a separate file for manual inspection
                with open(f"error_files/{file.filename}", 'w', encoding='utf-8') as error_file:                            
                    error_file.write(file_content)

            # Update the main progress bar
            pbar.update(1)
            
            # Add final statistics to logger
            logger.info(f"File {file.filename} processed: "
                       f"{identifiers_found} identifiers found, "
                       f"{total_lines_processed} lines processed")
    
    # Log final statistics
    logger.info(f"Processing completed: "
                f"{total_python_files} files, "
                f"{identifiers_found} identifiers found "
                f"({len(identifier_occurrences['added'])} added, "
                f"{len(identifier_occurrences['removed'])} removed, "
                f"{len(identifier_occurrences['unchanged'])} unchanged)")
    
    return identifier_occurrences

def process_file_content(content, is_notebook):
    original_lines = []
    code_lines = []
    
    if is_notebook:
        # Process Jupyter Notebook content
        for line in content.splitlines():
            # Keep track of line status
            if line.startswith('+') and not line.startswith('+++'):
                code_lines.append(line[1:])
                original_lines.append(('added', line[1:]))
            elif line.startswith('-') and not line.startswith('---'):
                code_lines.append(line[1:])
                original_lines.append(('removed', line[1:]))
            elif line.startswith("@"):
                # Preserve diff headers
                original_lines.append(('unchanged', re.sub(r'^\@\@.*\@\@', '', line)))
            else:
                code_lines.append(line.strip())
                original_lines.append(('unchanged', line.strip()))
    else:
        # Process regular Python file content
        for line in content.splitlines():
            # Keep track of line status
            if line.startswith('+') and not line.startswith('+++'):
                code_lines.append(line[1:])
                original_lines.append(('added', line[1:]))
            elif line.startswith('-') and not line.startswith('---'):
                code_lines.append(line[1:])
                original_lines.append(('removed', line[1:]))
            elif line.startswith("@"):
                # Preserve diff headers
                original_lines.append(('unchanged', re.sub(r'^\@\@.*\@\@', '', line)))
            else:
                code_lines.append(line)
                original_lines.append(('unchanged', line))
    
    return original_lines, code_lines

def get_notebook_content(notebook_data):
    try:
        notebook = json.loads(notebook_data)
        code_cells = [cell['source'] for cell in notebook['cells'] if cell['cell_type'] == 'code']
        return '\n'.join([''.join(source) for source in code_cells])
    except (json.JSONDecodeError, KeyError):
        logger.error("Error decoding Jupyter Notebook content")
        return ""

def analyze_github_commits(g: Github, db_path, identifiers_csv_path, output_path):
    identifiers = load_identifiers_from_csv(identifiers_csv_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    output_conn = sqlite3.connect(output_path)
    output_cursor = output_conn.cursor()
    output_cursor.execute('''
        CREATE TABLE IF NOT EXISTS commit_identifier_types (
            repo_name TEXT,
            commit_hash TEXT,
            file_path TEXT,
            identifier TEXT,
            type TEXT,
            flag TEXT
        )
    ''')
    
    cursor.execute("SELECT project_name, commit_hash FROM commit_analysis")
    commits = cursor.fetchall()
    repo_pool = {}
    for project_name, commit_hash in commits:
        print("\n====================================================")
        logger.info(f"Processing commit {commit_hash} in {project_name}")
        process_commit(g, project_name, commit_hash, repo_pool, output_cursor, fairness_identifiers)
        print("====================================================\n")

if __name__ == "__main__":
    g = Github(auth=Auth.Token(GH_TOKEN))
    DATABASE_PATH = '../commit_analysis.db'
    IDENTIFIERS_CSV_PATH = '../identifiers.csv'
    OUTPUT_DATABASE_PATH = '../commit_identifier_types.db'
    
    analyze_github_commits(
        g,
        DATABASE_PATH, 
        IDENTIFIERS_CSV_PATH, 
        OUTPUT_DATABASE_PATH
    )
