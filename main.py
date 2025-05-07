from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import pandas as pd
from difflib import get_close_matches

app = FastAPI()

# Load and prepare dataset
def load_and_prepare_job_data():
    skills_df = pd.read_csv(r'C:\Users\kulai\OneDrive\Desktop\Job-Skill Recommendation\job_skills.csv')
    linkedin_df = pd.read_csv(r'C:\Users\kulai\OneDrive\Desktop\Job-Skill Recommendation\linkedin_job_postings.csv')

    # Clean missing skill data
    skills_df['job_skills'] = skills_df['job_skills'].fillna('').astype(str)

    # Group skills by job_link
    job_skills_map = skills_df.groupby('job_link')['job_skills'].apply(
        lambda x: list(set([s.strip().lower() for s in ','.join(x).split(',') if s.strip()]))
    ).reset_index()

    # Merge with job titles
    job_roles_df = pd.merge(linkedin_df[['job_link', 'job_title']], job_skills_map, on='job_link')
    job_roles_df = job_roles_df.dropna().drop_duplicates('job_title')
    job_roles_df = job_roles_df[['job_title', 'job_skills']]
    job_roles_df.columns = ['title', 'required_skills']

    return job_roles_df

job_roles_df = load_and_prepare_job_data()

# Request body structure
class InputData(BaseModel):
    skills: List[str]
    interests: List[str]
    current_position: str
    desired_role: str

# Core recommendation logic
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

# FastAPI route
@app.post("/recommend")
def get_recommendation(data: InputData):
    missing_skills, message = recommend_skills(data.skills, data.desired_role)

    roadmap = []
    for i, skill in enumerate(missing_skills):
        roadmap.append({
            "step": i + 1,
            "description": f"Learn {skill} via platforms like Coursera, FreeCodeCamp, or YouTube"
        })

    return {
        "status": message,
        "missing_skills": missing_skills,
        "roadmap": roadmap
    }
    