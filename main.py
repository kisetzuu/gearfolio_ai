from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
import gdown
from difflib import get_close_matches
import os

app = FastAPI()

# === Google Drive File IDs ===
JOB_SKILLS_FILE_ID = "1om4qOQvcz28IWFAjhYKqg9M-VsePtnN5"
JOB_SUMMARY_FILE_ID = "1r_4dpd5i0ED89j15DSj39xSRc11dVIT_"
LINKEDIN_POSTINGS_FILE_ID = "1oYv4uueQgE7VVpa4Mjfj-8N0kOGrPAla"

# === Download CSV with streaming limit ===
def load_csv_from_gdrive(file_id: str, nrows=500) -> pd.DataFrame:
    try:
        print(f"⬇️ Downloading file ID: {file_id}")
        url = f"https://drive.google.com/uc?id={file_id}"
        output_path = f"/tmp/{file_id}.csv"
        if not os.path.exists(output_path):
            gdown.download(url, output_path, quiet=False)
        print(f"✅ File downloaded: {output_path}")
        return pd.read_csv(output_path, nrows=nrows)
    except Exception as e:
        print(f"❌ Error loading file {file_id}: {e}")
        raise

# === Load and prepare dataset ===
def load_and_prepare_job_data():
    print("📥 Loading job_skills.csv...")
    skills_df = load_csv_from_gdrive(JOB_SKILLS_FILE_ID, nrows=500)

    print("📥 Loading job_summary.csv...")
    summary_df = load_csv_from_gdrive(JOB_SUMMARY_FILE_ID, nrows=500)

    print("📥 Loading linkedin_job_postings.csv...")
    linkedin_df = load_csv_from_gdrive(LINKEDIN_POSTINGS_FILE_ID, nrows=500)

    # Check required columns
    required_checks = [
        (skills_df, "job_skills.csv", ['job_link', 'job_skills']),
        (summary_df, "job_summary.csv", ['job_link', 'job_summary']),
        (linkedin_df, "linkedin_job_postings.csv", ['job_link', 'job_title'])
    ]

    for df, name, cols in required_checks:
        for col in cols:
            if col not in df.columns:
                raise ValueError(f"❌ Missing column '{col}' in {name}")

    print("🧹 Cleaning and merging data...")

    # Clean and group skills
    skills_df['job_skills'] = skills_df['job_skills'].fillna('').astype(str)
    job_skills_map = skills_df.groupby('job_link')['job_skills'].apply(
        lambda x: list(set([s.strip().lower() for s in ','.join(x).split(',') if s.strip()]))
    ).reset_index()

    # Merge everything
    merged_df = linkedin_df[['job_link', 'job_title']].merge(job_skills_map, on='job_link', how='inner')
    merged_df = merged_df.merge(summary_df[['job_link', 'job_summary']], on='job_link', how='left')

    merged_df = merged_df.dropna(subset=['job_title', 'job_skills']).drop_duplicates('job_title')
    merged_df = merged_df[['job_title', 'job_skills', 'job_summary']]
    merged_df.columns = ['title', 'required_skills', 'summary']

    print("✅ Dataset loaded and prepared.")
    return merged_df

print("🚀 Starting app and preparing data...")
job_roles_df = load_and_prepare_job_data()

# === Input schema ===
class InputData(BaseModel):
    skills: List[str]
    interests: List[str]
    current_position: str
    desired_role: str

# === Core logic ===
def recommend_skills(user_skills, desired_role):
    titles = job_roles_df['title'].str.lower().tolist()
    closest = get_close_matches(desired_role.lower(), titles, n=1, cutoff=0.5)

    if not closest:
        return [], "Desired role not found. Try again with a different title.", ""

    row = job_roles_df[job_roles_df['title'].str.lower() == closest[0]].iloc[0]
    required = set(row['required_skills'])
    user_skills_set = set([s.strip().lower() for s in user_skills])
    missing = list(required - user_skills_set)

    return missing, f"Skills needed for {row['title']}", row['summary']

# === API endpoint ===
@app.post("/recommend")
def get_recommendation(data: InputData):
    missing_skills, message, summary = recommend_skills(data.skills, data.desired_role)

    roadmap = [
        {
            "step": i + 1,
            "description": f"Learn {skill} via platforms like Coursera, FreeCodeCamp, or YouTube"
        }
        for i, skill in enumerate(missing_skills)
    ]

    return {
        "status": message,
        "job_summary": summary,
        "missing_skills": missing_skills,
        "roadmap": roadmap
    }
