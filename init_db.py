# save as init_db.py
from services.db_service import FormSubmissionDB

db = FormSubmissionDB()
print("✓ Database initialized")

# Check tables
import sqlite3
conn = sqlite3.connect('form_submissions.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"Tables: {tables}")
conn.close()