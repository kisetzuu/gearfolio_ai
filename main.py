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
LINKEDIN_POSTINGS_FILE_ID = "1oYv4uueQgE7VVpa4Mjfj-8N0kOGrPAla"

# === Helper: Download CSV from Google Drive using gdown ===
def load_csv_from_gdrive(file_id: str) -> pd.DataFrame:
    url = f"https://drive.google.com/uc?id={file_id}"
    output_path = f"/tmp/{file_id}.csv"  # Safe path for Render
    if not os.path.exists(output_path):
        gdown.download(url, output_path, quiet=False)
    return pd.read_csv(output_path)

# === Load and Prepare Job Dataset ===
def load_and_prepare_job_data():
    skills_df = load_csv_from_gdrive(JOB_SKILLS_FILE_ID)
    linkedin_df = load_csv_from_gdrive(LINKEDIN_POSTINGS_FILE_ID)

    # Defensive column checks
    if 'job_skills' not in skills_df.columns or 'job_link' not in skills_df.columns:
        raise ValueError("Missing required columns in job_skills.csv")

    if 'job_title' not in linkedin_df.columns or 'job_link' not in linkedin_df.columns:
        raise ValueError("Missing required columns in linkedin_job_postings.csv")

    # Clean missing skill data
    skills_df['job_skills'] = skills_df['job_skills'].fillna('').astype(str)

    # Group skills by job_link
    job_skills_map = skills_df.groupby('job_link')['job_skills'].apply(
        lambda x: list(set([s.strip().lower() for s in ','.join(x).split(',') if s.strip()]))
    ).reset_index()

    # Merge with job titles
    job_roles_df = pd.merge(linkedin_df[['job_link', 'job_title']], job_skills_map, on='job_link')
    job_roles_df = job_roles_df.dropna(subset=['job_title', 'job_skills']).drop_duplicates('job_title')
    job_roles_df = job_roles_df[['job_title', 'job_skills']]
    job_roles_df.columns = ['title', 'required_skills']

    return job_roles_df

# Load dataset once on startup
job_roles_df = load_and_prepare_job_data()

# === Pydantic Request Schema ===
class InputData(BaseModel):
    skills: List[str]
    interests: List[str]
    current_position: str
    desired_role: str

# === Recommendation Logic ===
def recommend_skills(user_skills, desired_role):
    titles = job_roles_df['title'].str.lower().tolist()
    closest = get_close_matches(desired_role.lower(), titles, n=1, cutoff=0.5)

    if not closest:
        return [], "Desired role not found. Try again with a different title."

    row = job_roles_df[job_roles_df['title'].str.lower() == closest[0]]
    required = set(row.iloc[0]['required_skills'])
    user_skills_set = set([s.strip().lower() for s in user_skills])
    missing = list(required - user_skills_set)

    return missing, f"Skills needed for {row.iloc[0]['title']}"

# === FastAPI Endpoint ===
@app.post("/recommend")
def get_recommendation(data: InputData):
    missing_skills, message = recommend_skills(data.skills, data.desired_role)

    roadmap = [
        {
            "step": i + 1,
            "description": f"Learn {skill} via platforms like Coursera, FreeCodeCamp, or YouTube"
        }
        for i, skill in enumerate(missing_skills)
    ]

    return {
        "status": message,
        "missing_skills": missing_skills,
        "roadmap": roadmap
    }
