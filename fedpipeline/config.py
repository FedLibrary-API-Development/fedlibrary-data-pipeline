# config.py

API_URL = "http://localhost:8000/api/v1/resources/"

# API Configuration
API_CONFIG = {
    "LOGIN_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/users/login",
    "SCHOOLS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/schools",
    "INTEGRATION_USERS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/integration-users",
    "READINGS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/readings",
    "UNITS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/units",
    "UNIT_OFFERINGS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/unit-offerings",
    "TEACHING_SESSIONS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/teaching-sessions",
    "READING_LISTS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/reading-lists",
    "READING_LIST_USAGE_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/reading-list-usages",
    "READING_LIST_ITEMS_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/reading-list-items",
    "READING_LIST_ITEM_USAGE_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/reading-list-item-usages",
    "READING_UTILISATION_URL": "https://teambmockapi-569115112148.us-central1.run.app/api/v1/reading-utilisations",
}

# API Credentials
CREDENTIALS = {
    "email": "admin@example.edu",
    "password": "anypassword"
}


DB_CONFIG = {
    "DRIVER": "ODBC Driver 17 for SQL Server",
    "SERVER": "localhost\\SQLEXPRESS",
    "DATABASE": "eReserveData",
    "UID": "sa",
    "PWD": "tharu"
}
