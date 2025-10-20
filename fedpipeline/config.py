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
KNOWN_PREFIXES = {"BUACC","ITECH","SCGEO","MVAGC","HEALT","BULAW","SCMED","BUGEN","EXSCI","GLINT","ENGIN","EDMAS","SCENV","ENMEC","BACAP","ETCOR","MGGGC","SCCHM","MATHS","NURBN","LITCR","EDBED","EDBPE","BEHAV","CRJUS","WELRO","ATSGC","WELSI","SOSCI","HISOC","HMALS","OEEDU","VAPAP","VASAP","PSYCB","SCCOR","PHSED","COMMD","BUECO","BUMGT","FASTP","LITCI","MREGC","BUHRM","SCMET","ENMIN","ENCOR","UBCOL","SCBCH","SCBIO","BUHON","SOCIN","PHIGL","PHILO","EDDDE","SCMIC","SCVET","SOCIO","SSEXC","STATS","EEBED","PHSGL","INDSL","INDOL","EDFGC","SCMOL","SCHON","PSYCP","BUEXC","HMPRC","HUMOV","BUMKT","HCNUR","EDMED","EDRES","EXPHS","HCPAR","HEASC","HISTO","AABCA","BAHRS","MSWPG","SPMAN","VARTS","EDECE","JAPAG","OHSGC","BAXDC","FLMES","VATHR","VASDP","VAGTR","VAPPM","BSMAN","ENGGC","EDDOC","EDTAS","ENCIV","PSYCD","SCFST","SCSUS","VAMIN","MIDGD","HLTSC","EEDDE","FEAFN","ETCIV","ETMEC","SCBRW","EXESP","SCOND","BUENT","SCOHS","FLMGL","PSYCM","EDART","PSYCH","EDGCT","PAATF","PAATP","PAATS","PAATV","PACAC","PADWD","PAMTP","PAMTS","MTACT","MTDAN","MTHIS","MTPRO","MTSIN","MTSKI","MTTHR","PAAMS","PAATA","PAATC","BUHEA","SCEXC","ISEAP","MTPRP","MTSHW","BSHSP","EEBPE","BUTSM","FLMOL","IBLGC","BAFLM","BAHIS","HISGL","BAKIP","BALIT","BAPHL","EDHPE","EEZED","BASOC","BASSS","BATCC","BAWRT","CHSUG","CPPSV","ACACW","CAXDC","CVASP","EDMTH","NHPRH","GCSCS","HENAE","EXREH","HLWHS","CPPRO","CPPSD","CPPSS","BAEXC","MIDBM","NHPOT","NHPBM","NHPHS","NHPPS","HEALM","HEALN","HEALP","COMDT","ACALM","EDMST","EDMSP","HEASP","HEAPH","ISMAN","HENAA","HEMTL","HENAH","VCHAT","VACAP","HEALE","HEALA","HEALC","ALHLT","GPENT","GPMGT","GPMKT","GPFSP","EDBSP","CPPSA","GPENG","GPMAT","GPSIT","HEALO","HENAI","SCFSS","GPACC","GPECO","GPGEN","GPLAW","BSWUG","EDCEL","COOPB","COOPC","DATSC","HEANP","HEAAN","ECCEL","RESPJ","MONCI","MSWCF","CGCTP","CGDTP","HEACN","COOPI","COOPE","SCBFN","BADIG","COOPA","COOPT","ENGPG","ENGRG","BAENG","BAIND","VIEPS","SHPOE","MANSV","COOPS","COOPK","HNRBC","HENAG","ITWSU","EDPTH","AIWSU","BUDIP","DLGLM"}

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
    "PWD": "password"
}

# Page size to fetch data in batches
PAGE_SIZE = 1000

# Date filter configuration
DATE_FILTER_CONFIG = {
    "USE_YEARS_BACK": True,         # Set to True to use YEARS_BACK, False to use fixed dates
    "YEARS_BACK": 0.5,              # How many years back from today (0.5 = 6 months, 1.0 = 1 year)
    "START_DATE": "2023-01-01",     # Format: YYYY-MM-DD 
    "END_DATE": "2023-01-31"
}