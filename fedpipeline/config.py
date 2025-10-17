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

# UNIT Codes Prefixes
KNOWN_PREFIXES = {
    'AIWSU', 'BUACC', 'BUECO', 'BUHRM', 'BULAW', 'BUMGT', 'BUMKT', 'COOPB', 'COOPE', 'COOPI',
    'COOPC', 'COOPS', 'DATSC', 'ENGIN', 'ENGPG', 'ENGRG', 'ITECH', 'ITWSU', 'MATHS', 'MREGC',
    'SCBCH', 'SCBIO', 'SCBRW', 'SCCHM', 'SCCOR', 'SCENV', 'SCGEO', 'SCHON', 'SCMED', 'SCMIC',
    'SCMOL', 'SCSUS', 'SCVET', 'STATS', 'VIEPS'
}

# API Credentials
CREDENTIALS = {
    "email": "ta3@students.federation.edu.au",
    "password": "IamTG@8282828282"
}

# DB Configuration
DB_CONFIG = {
    "DRIVER": "ODBC Driver 17 for SQL Server",
    "SERVER": "localhost",
    "DATABASE": "eReserveData",
    "UID": "sa",
    "PWD": "YourPassword"
}

# Page size to fetch data in batches
PAGE_SIZE = 1000

# Date filter configuration
DATE_FILTER_CONFIG = {
    "USE_YEARS_BACK": True,         # Set to True to use YEARS_BACK, False to use fixed dates
    "YEARS_BACK": 0.5,              # How many years back from today (0.5 = 6 months, 1.0 = 1 year)
    "START_DATE": "2023-01-01",     # Format: YYYY-MM-DD 
    "END_DATE": "2023-01-31",
    "APPLY_TO_USAGE_TABLES": True   # Set to False to disable date filtering on usage tables
}