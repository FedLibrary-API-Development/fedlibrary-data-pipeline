import logging
import schedule
import time

def job():
    logging.info("Starting scheduled job...")

    from fedpipeline.jobs import (
        process_integration_users, process_schools, process_readings,
        process_units, process_teaching_sessions, process_reading_lists,
        process_reading_list_items, process_unit_offerings
    )
    from fedpipeline.usage_staging_processor import process_usage_data
    from fedpipeline.config import DATE_FILTER_CONFIG
    
    process_integration_users()
    process_schools()
    process_readings()
    process_units()
    process_teaching_sessions()
    process_reading_lists()
    process_reading_list_items()
    process_unit_offerings()
    
    # Process usage tables using staging method
    if DATE_FILTER_CONFIG.get("APPLY_TO_USAGE_TABLES", False):
        logging.info("Using staging processor for usage data")
        try:
            metrics = process_usage_data()
            logging.info(f"Usage data processing completed: {metrics}")
        except Exception as e:
            logging.error(f"Usage data processing failed: {e}")
            logging.info("Falling back to normal method")
            from fedpipeline.jobs import (
                process_reading_list_usage, process_reading_list_item_usage,
                process_reading_utilisation
            )
            process_reading_list_usage()
            process_reading_list_item_usage()
            process_reading_utilisation()
    else:
        # Use normal method when date filtering is disabled
        from fedpipeline.jobs import (
            process_reading_list_usage, process_reading_list_item_usage,
            process_reading_utilisation
        )
        process_reading_list_usage()
        process_reading_list_item_usage()
        process_reading_utilisation()

def start_scheduler():
    job()  # Run immediately at startup
    # Monthly run. Need to change this to use APScheduler
    schedule.every(1296000).minutes.do(job)
    logging.info("Scheduler started. Waiting for job trigger...")
    while True:
        schedule.run_pending()
        time.sleep(1)
