import csv
import logging
import os
import json
import shutil
import sqlite3
import re
import time
import sys
from typing import Any, Dict, List, Tuple, Generator
from pydriller import Repository
from pydriller.domain.commit import Commit, ModifiedFile
from urllib.parse import urlparse

CHECKPOINT_FILE = 'checkpoint.json'
CHUNK_SIZE = 200  # Numero di commit da caricare alla volta

def initialize_database(db_name: str = 'commit_analysis.db'):
	"""
	Initialize the SQLite database and create the necessary table.
	"""
	conn = sqlite3.connect(db_name)
	cursor = conn.cursor()
	
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS commit_analysis (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		project_name TEXT,
		commit_hash TEXT,
		commit_timestamp TEXT,
		author TEXT,
		affected_files TEXT,
		found_keywords TEXT,
		commit_type TEXT,
		commit_number INTEGER
	)
	''')
	
	cursor.execute('''
	CREATE TABLE IF NOT EXISTS checkpoint (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		repo_name TEXT,
		last_commit TEXT,
		total_time REAL
	)
	''')

	conn.commit()
	conn.close()

def save_record_to_db(record: Dict[str, Any], commit_number:int, db_name: str = 'commit_analysis.db'):
	"""
	Save a single record to the SQLite database.
	"""
	conn = sqlite3.connect(db_name)
	cursor = conn.cursor()
	
	cursor.execute('''
	INSERT INTO commit_analysis 
	(project_name, commit_hash, commit_timestamp, author, affected_files, found_keywords, commit_type, commit_number)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	''', (
		record['project_name'],
		record['commit_hash'],
		record['commit_timestamp'],
		record['author'],
		json.dumps(record['affected_files']),
		json.dumps(list(record['found_keywords'])),
		record['commit_type'],
		commit_number
	))
	
	conn.commit()
	conn.close()

def read_keywords_from_file(file_path: str = 'keywords.txt') -> List[str]:
	keywords = []
	try:
		with open(file_path, 'r') as file:
			for line in file:
				keyword = line.strip()
				if keyword and (not keyword.startswith('#')):
					# Escape special regex characters and replace * with .*
					keyword = re.escape(keyword).replace(r'\*', '.*')
					keywords.append(keyword)
	except FileNotFoundError:
		print(f"Warning: Keywords file '{file_path}' not found. Using an empty list.")
	except IOError as e:
		print(f"Error reading keywords file: {e}")

	return keywords

def read_repo_data(file_path: str) -> List[Tuple[str, str]]:
	repo_data = []
	with open(file_path, 'r') as file:
		reader = csv.reader(file)
		next(reader)  # Salta la prima riga (header)
		for row in reader:
			if len(row) >= 2:
				repo_name, url = row[0], row[1]
				repo_data.append((repo_name, url))
	return repo_data

def get_repo_name(url: str) -> str:
	parsed_url = urlparse(url)
	path = parsed_url.path.strip('/')
	return path.split('/')[-1].replace('.git', '')

def ensure_directory_exists(directory: str):
	if not os.path.exists(directory):
		os.makedirs(directory)
		logging.info(f"Created directory: {directory}")

def save_checkpoint(repo_name: str, commit_hash: str, total_time: float, db_name: str = 'commit_analysis.db'):
	conn = sqlite3.connect(db_name)
	cursor = conn.cursor()
	
	# Verifica se esiste già un checkpoint e, in tal caso, aggiornalo
	cursor.execute("SELECT * FROM checkpoint WHERE repo_name=?", (repo_name,))
	existing_checkpoint = cursor.fetchone()
	
	if existing_checkpoint:
		while True:
			try:
				conn = sqlite3.connect(db_name)
				cursor.execute('''
				UPDATE checkpoint
				SET last_commit=?, total_time=?
				WHERE repo_name=?
				''', (commit_hash, total_time, repo_name))
				break
			except Exception:
				conn.close()
				print("Error saving checkpoint")
	else:
		cursor.execute('''
		INSERT INTO checkpoint (repo_name, last_commit, total_time)
		VALUES (?,?,?)
		''', (repo_name, commit_hash, total_time))
	
	conn.commit()
	conn.close()

def load_checkpoint(db_name: str = 'commit_analysis.db') -> Tuple[str, str, float]:
	conn = sqlite3.connect(db_name)
	cursor = conn.cursor()
	
	cursor.execute("SELECT repo_name, last_commit, total_time FROM checkpoint")
	checkpoint_data = cursor.fetchone()
	
	conn.close()
	
	if checkpoint_data:
		return checkpoint_data[0], checkpoint_data[1], checkpoint_data[2]
	else:
		return None, None, 0.0

def print_status(msg: str):
	# Get the width of the terminal
	terminal_width = shutil.get_terminal_size().columns

	# Truncate or pad the message to fit the terminal width
	msg = msg.ljust(terminal_width)[:terminal_width]

	# Move cursor to the beginning of the line and print the message
	sys.stdout.write('\r' + msg)
	sys.stdout.flush()

def commit_generator(repo: Repository, last_commit: str = None) -> Generator[Commit, None, None]:
	commits = repo.traverse_commits()
	chunk = []
	found_last_commit = last_commit is None

	for commit in commits:
		if not found_last_commit:
			if commit.hash == last_commit:
				found_last_commit = True
			continue

		chunk.append(commit)
		if len(chunk) == CHUNK_SIZE:
			yield from chunk
			chunk = []

	if chunk:
		yield from chunk

def count_total_commits_safe(repo: Repository, last_commit: str = None) -> int:
	return repo.git.total_commits()

def classify_commit_advanced(commit_msg):
	msg = commit_msg.lower()
	
	categories = {
		'Bug Fixing': ['fix', 'bug', 'error', 'issue', 'crash', 'problem', 'fatal', 'defect', 'patch'],
		'New Feature': ['add', 'feature', 'implement', 'new', 'create', 'introduce', 'support'],
		'Enhancement': ['enhance', 'improve', 'optimize', 'update', 'upgrade', 'performance', 'boost', 'refine'],
		'Refactoring': ['refactor', 'clean', 'restructure', 'reorganize', 'rewrite', 'simplify', 'redesign']
	}
	
	return next((category for category, keywords in categories.items() 
		if any(keyword in msg for keyword in keywords)), 'Unknown')

def process_commits(repo_data: List[Tuple[str, str]], base_dir: str = '../cloned_repos/', keywords_file: str = '../keywords.txt', db_name: str = '../commit_analysis.db'):
	global KEYWORDS
	KEYWORDS = read_keywords_from_file(keywords_file)
	
	initialize_database(db_name)
	
	ensure_directory_exists(base_dir)
	last_repo, last_commit, total_time = load_checkpoint()
	
	start_index = next((i for i, (repo_name, _) in enumerate(repo_data) if repo_name == last_repo), 0)
	
	total_repos = len(repo_data)
	start_time = time.time() - total_time  # Subtract the saved total time

	for i, (repo_name, url) in enumerate(repo_data[start_index:], start=start_index):
		processed_commits = 0
		clone_path = os.path.join(base_dir)
		ensure_directory_exists(clone_path)
		print_status(f"Processing repository: {repo_name} ({url})\n")
		repo = None

		while repo is None:
			try:
				repo = Repository(url, clone_repo_to=clone_path)
			except Exception as e:
				logging.error(f"Error processing repository {repo_name}: {str(e)}")

		total_commits = 0
			
		for commit in commit_generator(repo, last_commit):
			
			if total_commits == 0 and repo.git is not None:
				total_commits = repo.git.total_commits()
			result = process_modified_files(commit.modified_files, commit)
			if result:
				save_record_to_db(result, processed_commits+1, db_name)
			
			processed_commits += 1
			current_time = time.time() - start_time
			save_checkpoint(repo_name, commit.hash, current_time)
			
			if processed_commits % 10 == 0 or processed_commits == total_commits:
				elapsed_time = time.time() - start_time
				repos_left = total_repos - (i + 1)
				commits_left = total_commits - processed_commits
				status = f"Current Repo: {repo_name} | "
				status += f"Repo: {i+1}/{total_repos} | Commit: {processed_commits}/{total_commits} | "
				status += f"Time: {elapsed_time:.2f}s | Repos left: {repos_left} | Commits left: {commits_left}\n"
				print_status(status)
		
		last_commit = None  # Reset for the next repository
	
	total_time = time.time() - start_time
	print("\n")  # Nuova riga alla fine per separare l'output finale
	logging.info(f"Mining completed. Total time: {total_time:.2f} seconds")
	logging.info(f"Processed {processed_commits} commits from {total_repos} repositories")

def process_modified_files(files: List[ModifiedFile], commit: Commit) -> Dict[str, Any]:
	result = {
		"project_name": get_repo_name(commit.project_path),
		"commit_hash": commit.hash,
		"commit_timestamp": commit.committer_date.isoformat(),
		"author": commit.author.name,
		"affected_files": [],
		"found_keywords": set(),
		"commit_type": classify_commit_advanced(commit.msg)
	}

	for file in files:
		for line in file.diff.splitlines():
			if line.startswith('+') or line.startswith('-'):  # Consider only added or removed lines
				for keyword in KEYWORDS:
					if re.search(rf"\b{keyword}\b", line, re.IGNORECASE):
						print(f"FOUND KEYWORD {keyword}")
						result["affected_files"].append(file.filename)
						result["found_keywords"].add(keyword)
						break  # Move to the next line after finding a keyword
	
	if result["affected_files"] != []:
		return result
	
	return None

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
	script_dir = os.path.dirname(os.path.abspath(__file__))
	csv_path = os.path.join(script_dir, '..', 'github_repositories.csv')
	repo_data = read_repo_data(csv_path)
	process_commits(repo_data)