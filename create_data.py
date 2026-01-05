import pandas as pd
import numpy as np

print("Creating sample job data...")

np.random.seed(42)

jobs = []
for i in range(2500):
    job = {
        'Job Title': np.random.choice(['Data Engineer', 'Senior Data Engineer', 'Lead Data Engineer']),
        'Salary Estimate': f'${np.random.randint(80,130)}K-${np.random.randint(130,180)}K',
        'Job Description': 'Python SQL Spark AWS Kafka Airflow ETL experience required',
        'Location': np.random.choice(['San Francisco, CA', 'New York, NY', 'Seattle, WA']),
        'Company Name': f'Company_{i}',
        'Rating': round(np.random.uniform(3.5, 4.8), 1)
    }
    jobs.append(job)

df = pd.DataFrame(jobs)
df.to_csv('DataEngineer.csv', index=False)
print(f"Created DataEngineer.csv with {len(df)} jobs!")