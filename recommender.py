from difflib import get_close_matches

def recommend_skills(user_skills, desired_role, job_roles_df):
    titles = job_roles_df['title'].str.lower().tolist()
    closest = get_close_matches(desired_role.lower(), titles, n=1, cutoff=0.5)
    
    if not closest:
        return [], "Desired role not found. Try again with a different title."

    # Find matching job title
    row = job_roles_df[job_roles_df['title'].str.lower() == closest[0]]
    required = set(row.iloc[0]['required_skills'])
    user_skills_set = set([s.strip().lower() for s in user_skills])
    missing = list(required - user_skills_set)

    return missing, f"Skills needed for {row.iloc[0]['title']}"
