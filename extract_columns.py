import pandas as pd

# Path to your CSV file
file_path = r'C:\Users\kulai\OneDrive\Desktop\Job-Skill Recommendation\linkedin_job_postings.csv'

# Load the CSV file
try:
    df = pd.read_csv(file_path)
    print("✅ Column names in linkedin_job_postings.csv:")
    for col in df.columns:
        print(f"- {col}")
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
