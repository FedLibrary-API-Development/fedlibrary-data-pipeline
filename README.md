# API to SQL Server Data Pipeline

This Python script fetches resource data from an API endpoint and inserts it into a SQL Server database on a scheduled interval using the `schedule` library.

## Features

- Fetch data from a REST API
- Insert into a local SQL Server database
- Runs every minute (adjustable)
- Logs success and errors

## Requirements

- Python 3.x
- SQL Server with the required table created
- ODBC Driver 17 for SQL Server

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/FedLibrary-API-Development/fedlibrary-data-pipeline-test.git
   cd fedlibrary-data-pipeline-test
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   pip install pytest
   ```

3. Execute the /sql/db.sql file in SQL Server. This will create eReserveData database and relevant tables.

4. Edit the connection string and API URL in `config.py` as needed.

5. Run the pipeline:
   ```
   python -m fedpipeline.main
   ```
6. If successful, you’ll see logging entries in pipeline.log.   

7. Run Unit Tests:
   Unit tests are located in the tests/ directory.
   ```
   pytest tests/
   ```