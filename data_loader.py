import pandas as pd

def load_and_prepare_job_data():
    # Load CSVs
    skills_df = pd.read_csv(r'C:\Users\kulai\OneDrive\Desktop\Job-Skill Recommendation\job_skills.csv')
    linkedin_df = pd.read_csv(r'C:\Users\kulai\OneDrive\Desktop\Job-Skill Recommendation\linkedin_job_postings.csv')

    # Fill missing skill data and make sure everything is string
    skills_df['job_skills'] = skills_df['job_skills'].fillna('').astype(str)

    # Aggregate skills per job_link
    job_skills_map = skills_df.groupby('job_link')['job_skills'].apply(
        lambda x: list(set([s.strip().lower() for s in ','.join(x).split(',') if s.strip()]))
    ).reset_index()

    # Merge job titles with skills
    job_roles_df = pd.merge(linkedin_df[['job_link', 'job_title']], job_skills_map, on='job_link')
    job_roles_df = job_roles_df.dropna().drop_duplicates('job_title')

    # Final cleaned dataframe
    job_roles_df = job_roles_df[['job_title', 'job_skills']]
    job_roles_df.columns = ['title', 'required_skills']
    
    return job_roles_df
