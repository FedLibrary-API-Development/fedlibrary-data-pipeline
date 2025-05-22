import logging
from fedpipeline.api_handler import fetch_data_from_api
from fedpipeline.db_handler import insert_records
from fedpipeline.config import API_CONFIG

def process_integration_users(token):
    users = fetch_data_from_api(API_CONFIG["INTEGRATION_USERS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["identifier"],
            item["attributes"]["roles"],
            item["attributes"]["first-name"],
            item["attributes"]["last-name"],
            item["attributes"]["email"],
            item["attributes"]["lti-consumer-user-id"],
            item["attributes"]["lti-lis-person-sourcedid"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
        )
        for item in users
    ]
    query = """
        INSERT INTO IntegrationUser (
            ereserve_id, identifier, roles, first_name, last_name, email,
            lti_consumer_user_id, lti_lis_person_sourcedid, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    insert_records(query, formatted, "IntegrationUser")

def process_schools(token):
    schools = fetch_data_from_api(API_CONFIG["SCHOOLS_URL"], token)
    formatted = [(item.get("id"), item["attributes"]["name"]) for item in schools]
    query = "INSERT INTO School (ereserve_id, name) VALUES (?, ?)"
    insert_records(query, formatted, "School")

def process_readings(token):
    readings = fetch_data_from_api(API_CONFIG["READINGS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["reading-title"],
            item["attributes"]["genre"],
            item["attributes"]["source-document-title"],
            item["attributes"]["article-number"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    units = fetch_data_from_api(API_CONFIG["UNITS_URL"], token)
    formatted = [
        (item.get("id"), item["attributes"]["code"], item["attributes"]["name"])
        for item in units
    ]
    query = "INSERT INTO Unit (ereserve_id, code, name) VALUES (?, ?, ?)"
    insert_records(query, formatted, "Unit")

def process_unit_offerings(token):
    offerings = fetch_data_from_api(API_CONFIG["UNIT_OFFERINGS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["unit-id"],
            item["attributes"]["reading-list-id"],
            item["attributes"]["source-unit-code"],
            item["attributes"]["source-unit-name"],
            item["attributes"]["source-unit-offering"],
            item["attributes"]["result"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    sessions = fetch_data_from_api(API_CONFIG["TEACHING_SESSIONS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["name"],
            item["attributes"]["start-date"],
            item["attributes"]["end-date"],
            item["attributes"]["archived"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    lists = fetch_data_from_api(API_CONFIG["READING_LISTS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["unit-id"],
            item["attributes"]["teaching-session-id"],
            item["attributes"]["name"],
            item["attributes"]["duration"],
            item["attributes"]["start-date"],
            item["attributes"]["end-date"],
            item["attributes"]["hidden"],
            item["attributes"]["usage-count"],
            item["attributes"]["item-count"],
            item["attributes"]["approved-item-count"],
            item["attributes"]["deleted"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    items = fetch_data_from_api(API_CONFIG["READING_LIST_ITEMS_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["list-id"],
            item["attributes"]["reading-id"],
            item["attributes"]["status"],
            item["attributes"]["hidden"],
            item["attributes"]["reading-utilisations-count"],
            item["attributes"]["reading-importance"],
            item["attributes"]["usage-count"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    usages = fetch_data_from_api(API_CONFIG["READING_LIST_USAGE_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["list-id"],
            item["attributes"]["integration-user-id"],
            item["attributes"]["item-usage-count"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    usages = fetch_data_from_api(API_CONFIG["READING_LIST_ITEM_USAGE_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["item-id"],
            item["attributes"]["list-usage-id"],
            item["attributes"]["integration-user-id"],
            item["attributes"]["utilisation-count"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
    utilisations = fetch_data_from_api(API_CONFIG["READING_UTILISATION_URL"], token)
    formatted = [
        (
            item.get("id"),
            item["attributes"]["integration-user-id"],
            item["attributes"]["item-id"],
            item["attributes"]["item-usage-id"],
            item["attributes"]["created-at"],
            item["attributes"]["updated-at"]
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
