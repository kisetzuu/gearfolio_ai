import os
import pandas as pd

def split_csv_by_size(file_path, max_mb=95):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    output_dir = f"{file_name}_split"
    os.makedirs(output_dir, exist_ok=True)

    df_iterator = pd.read_csv(file_path, chunksize=10000)
    part = 1
    current_chunk = pd.DataFrame()

    for chunk in df_iterator:
        current_chunk = pd.concat([current_chunk, chunk], ignore_index=True)

        temp_path = os.path.join(output_dir, f"{file_name}_part{part}.csv")
        current_chunk.to_csv(temp_path, index=False)

        size_mb = os.path.getsize(temp_path) / (1024 * 1024)

        if size_mb > max_mb:
            # Remove oversized file and split into next part
            os.remove(temp_path)
            current_chunk = chunk
            part += 1
            temp_path = os.path.join(output_dir, f"{file_name}_part{part}.csv")
            current_chunk.to_csv(temp_path, index=False)

    print(f"✅ Done splitting: {file_path} → {output_dir}/")

# === Run for all 3 files ===
split_csv_by_size("linkedin_job_postings.csv")
split_csv_by_size("job_summary.csv")
split_csv_by_size("job_skills.csv")
