import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_sample_logs(day, num_records=1000):
    """Generate sample web server logs for a given day"""

    # Sample data for realistic logs
    ips = ['192.168.1.100', '10.0.0.50', '203.0.113.10', '198.51.100.25', '172.16.0.10']
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    urls = ['/api/users', '/api/orders', '/static/css/style.css', '/index.html', '/api/products']
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    ]
    status_codes = [200, 200, 200, 200, 404, 500, 201, 204]  # Weighted toward 200

    base_date = datetime(2024, 1, 1) + timedelta(days=day-1)

    logs = []
    for i in range(num_records):
        timestamp = base_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        log_entry = {
            'timestamp': timestamp,
            'ip_address': random.choice(ips),
            'method': random.choice(methods),
            'url': random.choice(urls),
            'status_code': random.choice(status_codes),
            'response_size': random.randint(100, 50000),
            'user_agent': random.choice(user_agents)
        }
        logs.append(log_entry)

    df = pd.DataFrame(logs)
    df = df.sort_values('timestamp')
    return df

# Generate sample data for 3 days
os.makedirs('logs', exist_ok=True)

for day in range(1, 4):
    df = generate_sample_logs(day, 1000)
    df.to_csv(f'logs/access_log_day{day}.csv', index=False)
    print(f"Generated access_log_day{day}.csv with {len(df)} records")

print("Sample data generation complete!")