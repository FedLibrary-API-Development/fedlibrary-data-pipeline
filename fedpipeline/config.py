# API Configuration
API_CONFIG = {
    "LOGIN_URL": "https://learningresources-staging.federation.edu.au/public/v1/users/login",
    "SCHOOLS_URL": "https://learningresources-staging.federation.edu.au/public/v1/schools",
    "INTEGRATION_USERS_URL": "https://learningresources-staging.federation.edu.au/public/v1/integration-users",
    "READINGS_URL": "https://learningresources-staging.federation.edu.au/public/v1/readings",
    "UNITS_URL": "https://learningresources-staging.federation.edu.au/public/v1/units",
    "UNIT_OFFERINGS_URL": "https://learningresources-staging.federation.edu.au/public/v1/unit-offerings",
    "TEACHING_SESSIONS_URL": "https://learningresources-staging.federation.edu.au/public/v1/teaching-sessions",
    "READING_LISTS_URL": "https://learningresources-staging.federation.edu.au/public/v1/reading-lists",
    "READING_LIST_USAGE_URL": "https://learningresources-staging.federation.edu.au/public/v1/reading-list-usages",
    "READING_LIST_ITEMS_URL": "https://learningresources-staging.federation.edu.au/public/v1/reading-list-items",
    "READING_LIST_ITEM_USAGE_URL": "https://learningresources-staging.federation.edu.au/public/v1/reading-list-item-usages",
    "READING_UTILISATION_URL": "https://learningresources-staging.federation.edu.au/public/v1/reading-utilisations",
}

# API Credentials
CREDENTIALS = {
    "email": "youremail",
    "password": "yourPassword"
}

# DB Configuration
DB_CONFIG = {
    "DRIVER": "ODBC Driver 17 for SQL Server",
    "SERVER": "localhost\\SQLEXPRESS",
    "DATABASE": "eReserveData",
    "UID": "sa",
    "PWD": "tharu"
}

# Page size to fetch data in batches
PAGE_SIZE = 1000