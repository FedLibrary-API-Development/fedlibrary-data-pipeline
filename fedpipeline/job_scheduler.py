import logging
import schedule
import time
import traceback

def job():
    logging.info("Starting scheduled job...")

    from fedpipeline.jobs import (
        process_integration_users, process_schools, process_readings,
        process_units, process_teaching_sessions, process_reading_lists,
        process_reading_list_items, process_unit_offerings
    )
    from fedpipeline.usage_staging_processor import process_usage_data
    from fedpipeline.pipeline_run_manager import PipelineRunHistoryManager
    
    run_manager = PipelineRunHistoryManager()
    is_first_run = run_manager.is_first_run()   
    run_id = run_manager.start_run(is_initial_load=is_first_run)
    
    try:
        process_integration_users()
        process_schools()
        process_readings()
        process_units()
        process_teaching_sessions()
        process_reading_lists()
        process_reading_list_items()
        process_unit_offerings()
        
        # Process usage tables
        if is_first_run:
            logging.info("FIRST RUN DETECTED - Fetching ALL usage data")
            
            from fedpipeline.jobs import (
                process_reading_list_usage, process_reading_list_item_usage,
                process_reading_utilisation
            )
            
            process_reading_list_usage()
            process_reading_list_item_usage()
            process_reading_utilisation()

            if run_id:
                run_manager.end_run_success(run_id)

            logging.info("FIRST RUN COMPLETED - Next run will use date filtering")
            
        else:
            logging.info("SUBSEQUENT RUN DETECTED - Using date filtering for usage data")
            try:
                metrics = process_usage_data()
                
                if run_id:
                    run_manager.end_run_success(run_id, metrics)
                
                logging.info(f"Usage data processing completed: {metrics}")
                
            except Exception as e:
                logging.error(f"Usage data processing failed: {e}")
                logging.error(f"Stack trace: {traceback.format_exc()}")

                if run_id:
                    run_manager.end_run_failure(run_id, str(e), traceback.format_exc())
                
                logging.info("Falling back to normal method")
                from fedpipeline.jobs import (
                    process_reading_list_usage, process_reading_list_item_usage,
                    process_reading_utilisation
                )
                process_reading_list_usage()
                process_reading_list_item_usage()
                process_reading_utilisation()
        
        # Display run stats
        stats = run_manager.get_run_statistics()
        logging.info("PIPELINE RUN STATISTICS:")
        logging.info(f"  Total runs: {stats.get('total_runs', 0)}")
        logging.info(f"  Successful: {stats.get('successful_runs', 0)}")
        logging.info(f"  Failed: {stats.get('failed_runs', 0)}")
        logging.info(f"  Last success: {stats.get('last_success_time', 'N/A')}")
        
    except Exception as e:
        logging.error(f"Pipeline job failed with error: {e}")
        logging.error(f"Stack trace: {traceback.format_exc()}")
        
        if run_id:
            run_manager.end_run_failure(run_id, str(e), traceback.format_exc())
        
        raise

def start_scheduler():
    job()  # Run immediately at startup
    # Monthly run. Need to change this to use APScheduler
    schedule.every(1296000).minutes.do(job)
    logging.info("Scheduler started. Waiting for job trigger...")
    while True:
        schedule.run_pending()
        time.sleep(1)
