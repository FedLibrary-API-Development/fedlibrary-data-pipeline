import logging
from fedpipeline.api_handler import fetch_data_from_api
from fedpipeline.db_handler import insert_records
from fedpipeline.config import API_CONFIG
from fedpipeline.config import PAGE_SIZE

def fetch_all_pages(url, token):
    all_items = []
    while url:
        response = fetch_data_from_api(url, token)
        data = response.json().get("data", [])
        links = response.json().get("links", {})
        logging.info(f"Fetched {len(data)} items from {url}")
        if not data:
            break
        all_items.extend(data)
        url = links.get("next")
    return all_items

def process_integration_users(token):
    url = f"{API_CONFIG['INTEGRATION_USERS_URL']}?page[size]={PAGE_SIZE}"
    all_users = fetch_all_pages(url, token)

    formatted = [
        (
            item.get("id"),
            item["attributes"].get("identifier"),
            item["attributes"].get("roles"),
            item["attributes"].get("first-name"),
            item["attributes"].get("last-name"),
            item["attributes"].get("email"),
            item["attributes"].get("lti-consumer-user-id"),
            item["attributes"].get("lti-lis-person-sourcedid"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at"),
        )
        for item in all_users
    ]
    query = """
        INSERT INTO IntegrationUser (
            ereserve_id, identifier, roles, first_name, last_name, email,
            lti_consumer_user_id, lti_lis_person_sourcedid, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "IntegrationUser")

def process_schools(token):
    url = f"{API_CONFIG['SCHOOLS_URL']}?page[size]={PAGE_SIZE}"
    schools = fetch_all_pages(url, token)
    formatted = [(item.get("id"), item["attributes"].get("name")) for item in schools]
    query = "INSERT INTO School (ereserve_id, name) VALUES (?, ?)"
    insert_records(query, formatted, "School")

def process_readings(token):
    url = f"{API_CONFIG['READINGS_URL']}?page[size]={PAGE_SIZE}"
    readings = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("reading-title"),
            item["attributes"].get("genre"),
            item["attributes"].get("source-document-title"),
            item["attributes"].get("article-number"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in readings
    ]
    query = """
        INSERT INTO Reading (
            ereserve_id, reading_title, genre, source_document_title,
            article_number, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "Reading")

def process_units(token):
    school_url = f"{API_CONFIG['SCHOOLS_URL']}?page[size]={PAGE_SIZE}"
    school_ids = [item.get("id") for item in fetch_all_pages(school_url, token)]
    
    all_units = []

    # Note: The 'school_id' is not included in the unit data returned by the API.
    # However, the API allows filtering units by 'school_id', so we fetch units for each school separately
    # and manually associate the 'school_id' with each unit record during insertion.
    for school_id in school_ids:
        logging.info(f"Getting values for school ID: {school_id}")
        url = f"{API_CONFIG['UNITS_URL']}?filter[school_id]={school_id}&page[size]={PAGE_SIZE}"
        units = fetch_all_pages(url, token)
        all_units.extend([
            (item.get("id"), item["attributes"].get("code"), item["attributes"].get("name"), school_id)
            for item in units
        ])
    if all_units:
        query = "INSERT INTO Unit (ereserve_id, code, name, school_id) VALUES (?, ?, ?, ?)"
        insert_records(query, all_units, "Unit")

def process_unit_offerings(token):
    url = f"{API_CONFIG['UNIT_OFFERINGS_URL']}?page[size]={PAGE_SIZE}"
    offerings = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("unit-id"),
            item["attributes"].get("reading-list-id"),
            item["attributes"].get("source-unit-code"),
            item["attributes"].get("source-unit-name"),
            item["attributes"].get("source-unit-offering"),
            item["attributes"].get("result"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in offerings
    ]
    query = """
        INSERT INTO UnitOffering (
            ereserve_id, unit_id, reading_list_id, source_unit_code,
            source_unit_name, source_unit_offering, result, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "UnitOffering")

def process_teaching_sessions(token):
    url = f"{API_CONFIG['TEACHING_SESSIONS_URL']}?page[size]={PAGE_SIZE}"
    sessions = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("name"),
            item["attributes"].get("start-date"),
            item["attributes"].get("end-date"),
            item["attributes"].get("archived"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in sessions
    ]
    query = """
        INSERT INTO TeachingSession (
            ereserve_id, name, start_date, end_date,
            archived, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "TeachingSession")

def process_reading_lists(token):
    url = f"{API_CONFIG['READING_LISTS_URL']}?page[size]={PAGE_SIZE}"
    lists = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("unit-id"),
            item["attributes"].get("teaching-session-id"),
            item["attributes"].get("name"),
            item["attributes"].get("duration"),
            item["attributes"].get("start-date"),
            item["attributes"].get("end-date"),
            item["attributes"].get("hidden"),
            item["attributes"].get("usage-count"),
            item["attributes"].get("item-count"),
            item["attributes"].get("approved-item-count"),
            item["attributes"].get("deleted"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in lists
    ]
    query = """
        INSERT INTO ReadingList (
            ereserve_id, unit_id, teaching_session_id, name, duration,
            start_date, end_date, hidden, usage_count, item_count,
            approved_item_count, deleted, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "ReadingList")

def process_reading_list_items(token):
    url = f"{API_CONFIG['READING_LIST_ITEMS_URL']}?page[size]={PAGE_SIZE}"
    items = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("list-id"),
            item["attributes"].get("reading-id"),
            item["attributes"].get("status"),
            item["attributes"].get("hidden"),
            item["attributes"].get("reading-utilisations-count"),
            item["attributes"].get("reading-importance"),
            item["attributes"].get("usage-count"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in items
    ]
    query = """
        INSERT INTO ReadingListItem (
            ereserve_id, list_id, reading_id, status, hidden,
            reading_utilisations_count, reading_importance,
            usage_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "ReadingListItem")

def process_reading_list_usage(token):
    url = f"{API_CONFIG['READING_LIST_USAGE_URL']}?page[size]={PAGE_SIZE}"
    usages = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("list-id"),
            item["attributes"].get("integration-user-id"),
            item["attributes"].get("item-usage-count"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in usages
    ]
    query = """
        INSERT INTO ReadingListUsage (
            ereserve_id, list_id, integration_user_id,
            item_usage_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "ReadingListUsage")

def process_reading_list_item_usage(token):
    url = f"{API_CONFIG['READING_LIST_ITEM_USAGE_URL']}?page[size]={PAGE_SIZE}"
    usages = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("item-id"),
            item["attributes"].get("list-usage-id"),
            item["attributes"].get("integration-user-id"),
            item["attributes"].get("utilisation-count"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in usages
    ]
    query = """
        INSERT INTO ReadingListItemUsage (
            ereserve_id, item_id, list_usage_id,
            integration_user_id, utilisation_count,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "ReadingListItemUsage")

def process_reading_utilisation(token):
    url = f"{API_CONFIG['READING_UTILISATION_URL']}?page[size]={PAGE_SIZE}"
    utilisations = fetch_all_pages(url, token)
    formatted = [
        (
            item.get("id"),
            item["attributes"].get("integration-user-id"),
            item["attributes"].get("item-id"),
            item["attributes"].get("item-usage-id"),
            item["attributes"].get("created-at"),
            item["attributes"].get("updated-at")
        )
        for item in utilisations
    ]
    query = """
        INSERT INTO ReadingUtilisation (
            ereserve_id, integration_user_id, item_id,
            item_usage_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "ReadingUtilisation")
