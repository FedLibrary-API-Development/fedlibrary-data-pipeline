import logging
import schedule
import time
from fedpipeline.api_handler import fetch_data_from_api
from fedpipeline.db_handler import insert_records
from fedpipeline.config import API_CONFIG

def job():
    logging.info("Starting scheduled job...")

    from fedpipeline.jobs import (
        process_integration_users, process_schools, process_readings,
        process_units, process_teaching_sessions, process_reading_lists,
        process_reading_list_items, process_reading_list_usage,
        process_reading_list_item_usage, process_reading_utilisation,
        process_unit_offerings
    )
    
    process_integration_users()
    process_schools()
    process_readings()
    process_units()
    process_teaching_sessions()
    process_reading_lists()
    process_reading_list_items()
    process_reading_list_usage()
    process_reading_list_item_usage()
    process_reading_utilisation()
    process_unit_offerings()

def start_scheduler():
    job()  # Run immediately at startup
    # Monthly run. Need to change this to use APScheduler
    schedule.every(1296000).minutes.do(job)
    logging.info("Scheduler started. Waiting for job trigger...")
    while True:
        schedule.run_pending()
        time.sleep(1)
