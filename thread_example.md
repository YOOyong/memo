```python
from datetime import datetime, timedelta

# 이걸 common 에 넣는다??
def split_date_range(start_date, end_date, num_parts):
    delta_hours = 24 // num_parts
    current_date = start_date
    date_ranges = []

    for i in range(num_parts):
        next_date = current_date + timedelta(hours=delta_hours)
        if next_date > end_date:
            next_date = end_date  # Ensure the last interval doesn't go beyond the end_date

        date_ranges.append((current_date, next_date))
        current_date = next_date

    return date_ranges

# Example usage
start_date = datetime(2022, 10, 31, 0, 0, 0)
end_date = datetime(2022, 11, 1, 0, 0, 0)
num_parts = 3  # Adjust to the number of desired hourly intervals

date_ranges = split_date_range(start_date, end_date, num_parts)

for i, (start, end) in enumerate(date_ranges, start=1):
    print(f"Interval {i}: {start} - {end}")
```


```python
import cx_Oracle
import threading

# Replace these with your Oracle database connection details
oracle_user = "your_username"
oracle_password = "your_password"
oracle_dsn = "your_dsn"  # Database Source Name

# Define your queries
queries = [
    "SELECT * FROM your_table1",
    "SELECT * FROM your_table2",
    # Add more queries as needed
]

def execute_query(thread_number, query):
    # Function to execute a single query
    connection = cx_Oracle.connect(oracle_user, oracle_password, oracle_dsn)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    print(f"Thread {thread_number}: Query '{query}' executed successfully. Result: {result}")

def main():
    # Create a thread for each query
    threads = [threading.Thread(target=execute_query, args=(i, query)) for i, query in enumerate(queries, start=1)]

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
```
