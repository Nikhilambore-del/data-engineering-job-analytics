import pandas as pd
import numpy as np
from datetime import datetime

print("\n" + "="*60)
print("DATA ENGINEERING JOB MARKET ANALYSIS")
print("="*60 + "\n")

# Load data
print("Loading data...")
df = pd.read_csv('DataEngineer.csv')
print(f"Loaded {len(df)} job listings\n")

# Clean column names
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Extract salary
def get_salary(sal_str):
    if pd.isna(sal_str):
        return None, None
    nums = [int(x) for x in str(sal_str).replace('K','000').replace('$','').split('-') if x.replace('000','').isdigit()]
    if len(nums) >= 2:
        return nums[0], nums[1]
    return None, None

print("Extracting salaries...")
df[['min_salary', 'max_salary']] = df['salary_estimate'].apply(lambda x: pd.Series(get_salary(x)))
df['avg_salary'] = (df['min_salary'] + df['max_salary']) / 2

# Extract location
print("Extracting locations...")
if 'location' in df.columns:
    df['city'] = df['location'].str.split(',').str[0]
    df['state'] = df['location'].str.split(',').str[-1].str.strip()

# Extract skills
print("Extracting skills...")
if 'job_description' in df.columns:
    skills = ['python', 'sql', 'spark', 'kafka', 'aws', 'azure', 'airflow', 'docker']
    for skill in skills:
        df[f'skill_{skill}'] = df['job_description'].str.lower().str.contains(skill).astype(int)

# Analytics
print("\n" + "="*60)
print("KEY INSIGHTS")
print("="*60 + "\n")

print(f"Total Jobs: {len(df)}")
print(f"Average Salary: ${df['avg_salary'].mean():,.0f}")
print(f"Salary Range: ${df['min_salary'].min():,.0f} - ${df['max_salary'].max():,.0f}")

if 'city' in df.columns:
    print(f"\nTop 5 Cities:")
    for i, (city, count) in enumerate(df['city'].value_counts().head().items(), 1):
        print(f"  {i}. {city}: {count} jobs")

skill_cols = [c for c in df.columns if c.startswith('skill_')]
if skill_cols:
    print(f"\nTop Skills:")
    for i, (skill, count) in enumerate(df[skill_cols].sum().sort_values(ascending=False).head().items(), 1):
        pct = (count/len(df))*100
        print(f"  {i}. {skill.replace('skill_','').upper()}: {int(count)} jobs ({pct:.0f}%)")

# Save cleaned data
df.to_csv('cleaned_job_data.csv', index=False)
print(f"\nSaved cleaned data to: cleaned_job_data.csv")
print("\n" + "="*60)
print("ANALYSIS COMPLETE!")
print("="*60 + "\n")