import logging
import schedule
import time
from fedpipeline.api_handler import get_token, fetch_data_from_api
from fedpipeline.db_handler import insert_records
from fedpipeline.config import API_CONFIG

def run_entity_job(name, url, format_fn, query):
    token = get_token()
    if not token:
        logging.error(f"Skipping {name} job due to missing token.")
        return
    items = fetch_data_from_api(url, token)
    formatted = [format_fn(i) for i in items]
    insert_records(query, formatted, name)

def job():
    logging.info("Starting scheduled job...")
    token = get_token()
    if not token:
        logging.error("Job aborted due to missing token.")
        return

    from fedpipeline.jobs import (
        process_integration_users, process_schools, process_readings,
        process_units, process_teaching_sessions, process_reading_lists,
        process_reading_list_items, process_reading_list_usage,
        process_reading_list_item_usage, process_reading_utilisation,
        process_unit_offerings
    )

    process_integration_users(token)
    process_schools(token)
    process_readings(token)
    process_units(token)
    process_teaching_sessions(token)
    process_reading_lists(token)
    process_reading_list_items(token)
    process_reading_list_usage(token)
    process_reading_list_item_usage(token)
    process_reading_utilisation(token)
    process_unit_offerings(token)

def start_scheduler():
    schedule.every(1).minutes.do(job)
    logging.info("Scheduler started. Waiting for job trigger...")
    while True:
        schedule.run_pending()
        time.sleep(1)
